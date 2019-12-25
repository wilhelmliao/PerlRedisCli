use strict;

# the Redis reply types, see https://github.com/redis/hiredis/blob/master/read.h
use constant {
  REDIS_REPLY_STRING  => 1,
  REDIS_REPLY_ARRAY   => 2,
  REDIS_REPLY_INTEGER => 3,
  REDIS_REPLY_NIL     => 4,
  REDIS_REPLY_STATUS  => 5,
  REDIS_REPLY_ERROR   => 6,
  REDIS_REPLY_DOUBLE  => 7,
  REDIS_REPLY_BOOL    => 8,
  REDIS_REPLY_VERB    => 9,
  REDIS_REPLY_MAP     => 9,
  REDIS_REPLY_SET     => 10,
  REDIS_REPLY_ATTR    => 11,
  REDIS_REPLY_PUSH    => 12,
  REDIS_REPLY_BIGNUM  => 13,
};


{
  # represents Redis reply
  package RedisReply;


  sub new {
    my $class = shift;
    my $self = {
      _type  => shift,
      _value => shift,
    };
    bless $self, $class;
    return $self;
  }

  sub type {
    my $self = $_[0];

    return $self->{_type};
  }

  sub value {
    my $self = $_[0];

    return $self->{_value};
  }
}
{
  # provides a buffer for receive Redis reply buffer
  package RedisReplyBuffer;

  sub new {
    my $class = shift;
    my $self = {
      _buffer => undef,
      _offset => undef,
    };
    bless $self, $class;
    return $self;
  }

  sub append {
    my ( $self, $data, $offset ) = @_;
    return unless defined $data;

    if (defined $offset) {
      if ($offset < 0) {
        die "error: Invalid argument offset $offset\n";
      }
    } else {
      $offset = 0;
    }

    my $buffer = $self->{_buffer};

    if (defined $buffer) {
      $self->{_buffer} = $buffer . substr($data, $offset);
    } else {
      $self->{_buffer} = $data;
      $self->{_offset} = $offset;
    }
  }

  sub eof {
    my $self = $_[0];

    my $buffer = $self->{_buffer};
    if (defined $buffer) {
      my $offset = $self->{_offset};
      return ((length($buffer) - $offset) == 0);
    }
    return undef;
  }

  sub size {
    my $self = $_[0];

    my $buffer = $self->{_buffer};
    if (defined $buffer) {
      my $offset = $self->{_offset};
      return length($buffer) - $offset;
    }
    return 0;
  }

  sub readToken {
    my ( $self, $delimiter ) = @_;

    my $data = $self->{_buffer};
    if (defined $data) {
      my $offset = $self->{_offset};
      # if not eof?
      if (length($data) > $offset) {
        my $pos    = index($data, $delimiter, $offset);
        if ($pos > 0) {
          my $value = substr($data, $offset, $pos - $offset);

          # update offset
          $self->{_offset} = $pos + length($delimiter);
          return $value;
        }
      }
    }
    return undef;
  }

  sub readBytes {
    my ( $self, $length ) = @_;

    my $data = $self->{_buffer};
    if (defined $data) {
      my $offset = $self->{_offset};
      if (length($data) > $offset) {
        my $size = length($data) - $offset;

        if ($length <= $size) {
          my $value = substr($data, $offset, $length);

          # update offset
          $self->{_offset} = $offset + $length;
          return $value;
        }
      }
    }
    return undef;
  }

  sub skip {
    my ( $self, $length ) = @_;

    my $data = $self->{_buffer};
    if (defined $data) {
      my $offset = $self->{_offset};
      if (length($data) > $offset) {
        my $size = length($data) - $offset;

        if ($length <= $size) {
          # update offset
          $self->{_offset} = $offset + $length;
        }
      }
    }
  }

  sub shirink {
    my ( $self, $forcible ) = @_;

    my $data = $self->{_buffer};
    if (defined $data) {
      my $offset = $self->{_offset};
      if (length($data) > $offset) {
        if ($forcible || $offset > 256) {
          $self->{_buffer} = substr($data, $offset);
          $self->{_offset} = 0;

          return 1;
        }
      }
    }
    return undef;
  }

  sub clear {
    my ( $self, $data, $offset ) = @_;

    $self->{_buffer} = $data;
    $self->{_offset} = $offset;
  }
}
{
  # provides a stack for parsing Redis reply
  package Stack;

  sub new {
    my $class = shift;
    my $self = {
      _stack => undef,
    };
    bless $self, $class;
    return $self;
  }

  sub size {
    my $self = $_[0];

    my $stack = $self->{_stack};
    if (defined $stack) {
      return scalar @$stack;
    }
    return 0;
  }

  sub push {
    my ( $self, $item ) = @_;
    return unless defined $item;

    my $stack = $self->{_stack};
    if (!defined $stack) {
      $self->{_stack} = [ $item ];
    } else {
      push @$stack, $item;
    }
    return $item;
  }

  sub pop {
    my $self = $_[0];

    my $stack = $self->{_stack};
    if (defined $stack) {
      return pop @$stack;
    }
    return undef;
  }

  sub peek {
    my $self = $_[0];

    if (defined $self->{_stack}) {
      my @stack = @{ $self->{_stack} };
      my $last  = $#stack;
      return $stack[$last];
    }
    return undef;
  }

  sub clear {
    my $self = $_[0];

    $self->{_stack} = undef;
  }
}
{
  # provides a builder for reading and parsing Redis reply buffer
  package RedisReplyBuilder;

  use constant DELIMITER => "\r\n";

  sub new {
    my $class = shift;
    my $self = {
      _reply => undef,
      _stack => new Stack(),
                # { type=> type, size=> size, value=> value }
    };
    bless $self, $class;
    return $self;
  }

  sub reply {
    my $self = $_[0];

    return $self->{_reply};
  }

  sub resolve {
    my ( $self, $buffer ) = @_;
    return unless defined $buffer;

    my $stack = $self->{_stack};

    my $reply = __resolve($self, $buffer);
    if (defined $reply) {
      $stack->clear();
      $self->{_reply} = $reply;
    }
    return (defined $reply);
  }

  sub __resolve {
    my ( $self, $buffer ) = @_;

    my $stack = $self->{_stack};

    if ($stack->size == 0) {
      my $type = $buffer->readBytes(1);
      return unless defined $type;

      if ($type eq '+') {
        # resolve status
        my $reply = __resolveStatusReply($self, $buffer);
        if (!defined $reply) {
          $stack->push({ type=> $type });
        }
        return $reply;
      } elsif ($type eq '-') {
        # resolve error
        my $reply = __resolveErrorReply($self, $buffer);
        if (!defined $reply) {
          $stack->push({ type=> $type });
        }
        return $reply;
      } elsif ($type eq ':') {
        # resolve integer
        my $reply = __resolveIntegerReply($self, $buffer);
        if (!defined $reply) {
          $stack->push({ type=> $type });
        }
        return $reply;
      } elsif ($type eq '$') {
        # resolve string
        my $state = { type=> $type };
        my $reply = __resolveStringReply($self, $buffer, $state);
        if (!defined $reply) {
          $stack->push($state);
        }
        return $reply;
      } elsif ($type eq '*') {
        # resolve array
        my $state = { type=> $type };
        my $reply = __resolveArrayReply($self, $buffer, $state);
        if (!defined $reply) {
          $stack->push($state);
        }
        return $reply;
      } else {
        die "error: Invalid type flag 0x".unpack('H*', $type)."\n";
      }
    } else {
      # peek the stack
      my $state = $self->{_stack}->peek();

      if (!defined $state) {
        die "error: Cannot parse datagram\n";
      }

      my $type = $state->{type};

      if ($type eq '+') {
        # resolve status
        my $reply = __resolveStatusReply($self, $buffer);
        return $reply;
      } elsif ($type eq '-') {
        # resolve error
        my $reply = __resolveErrorReply($self, $buffer);
        return $reply;
      } elsif ($type eq ':') {
        # resolve integer
        my $reply = __resolveIntegerReply($self, $buffer);
        if (defined $reply) {
          $stack->pop();
          return $reply;
        }
        return $reply;
      } elsif ($type eq '$') {
        # resolve string
        my $reply = __resolveStringReply($self, $buffer, $state);
        if (defined $reply) {
          $stack->pop();
        }
        return $reply;
      } elsif ($type eq '*') {
        # resolve array
        $stack->pop();
        my $reply = __resolveArrayReply($self, $buffer, $state);
        if (!defined $reply) {
          $stack->push($state);
        }
        return $reply;
      }
    }
  }

  sub __resolveStatusReply {
    my ( $self, $buffer ) = @_;

    my $token = $buffer->readToken(DELIMITER);
    if (defined $token) {
      my $reply = new RedisReply(::REDIS_REPLY_STATUS, $token);
      $buffer->shirink();
      return $reply;
    }
    return undef;
  }

  sub __resolveErrorReply {
    my ( $self, $buffer ) = @_;

    my $token = $buffer->readToken(DELIMITER);
    if (defined $token) {
      my $reply = new RedisReply(::REDIS_REPLY_ERROR, $token);
      $buffer->shirink();
      return $reply;
    }
    return undef;
  }

  sub __resolveIntegerReply {
    my ( $self, $buffer ) = @_;

    my $token = $buffer->readToken(DELIMITER);

    if (defined $token) {
      my $reply = new RedisReply(::REDIS_REPLY_INTEGER, int($token));
      $buffer->shirink();
      return $reply;
    }
    return undef;
  }

  sub __resolveStringReply {
    my ( $self, $buffer, $state ) = @_;

    # read the size of the string
    my $size = $state->{size};
    if (!defined $size) {
      my $token = $buffer->readToken(DELIMITER);
      return unless defined $token;

      if ($token eq '-1') {
        my $reply = new RedisReply(::REDIS_REPLY_NIL, undef);
        $buffer->shirink();
        return $reply;
      } else {
        $size = int($token);
        $state->{size} = $size;
      }
    }

    # read string
    if ($buffer->size >= $size + length(DELIMITER)) {
      my $token     = $buffer->readBytes($size);
      my $delimiter = $buffer->readBytes(length(DELIMITER));
      die "error: Invalid data\n" if $delimiter ne DELIMITER;

      my $reply = new RedisReply(::REDIS_REPLY_STRING, $token);
      $buffer->shirink();
      return $reply;
    }
    return undef;
  }

  sub __resolveArrayReply {
    my ( $self, $buffer, $state ) = @_;

    # read the size of the string
    my $size = $state->{size};
    if (!defined $size) {
      my $token = $buffer->readToken(DELIMITER);
      return unless defined $token;

      if ($token eq '-1') {
        my $reply = new RedisReply(::REDIS_REPLY_NIL, undef);
        $buffer->shirink();
        return $reply;
      } elsif ($token eq '0') {
        my $reply = new RedisReply(::REDIS_REPLY_ARRAY, []);
        $buffer->shirink();
        return $reply;
      }else {
        $size = int($token);
        $state->{size}  = $size;
        $state->{value} = [];
      }
    }

    # read elements
    {
      my $list = $state->{value};
      my @list = @$list;
      while ($buffer->size > 0) {
        my $element = __resolve($self, $buffer);
        last unless defined $element;

        push @list, $element;
        if (scalar(@list) == $size) {
          my $reply = new RedisReply(::REDIS_REPLY_ARRAY, \@list);
          $buffer->shirink();
          return $reply;
        }
      }
      $state->{value} = \@list;
    }
    return undef;
  }
}
{
  # provides a set operators for Redis client
  package Redis;
  use IO::Socket::INET;

  use constant BUFFER_SIZE => 1024;

  sub new {
    my $class = shift;
    my $self = {
      _host    => shift || "127.0.0.1",
      _port    => shift || 6379,
      _socket  => undef,
      _buffer  => new RedisReplyBuffer(),
      _builder => new RedisReplyBuilder(),
    };
    bless $self, $class;
    return $self;
  }

  # builds a connection to Redis
  sub connect {
    my ( $self ) = @_;

    my $socket = IO::Socket::INET->new( PeerHost => $self->{_host},
                                        PeerPort => $self->{_port},
                                        Proto    => 'tcp')
            or die "error: Cannot connect to server '$self->{_host}:$self->{_port}'\n";

    $socket->autoflush(1);
    $socket->blocking(0);

    $self->{_socket} = $socket;
  }

  # executes a command
  sub exec {
    my $self    = shift;

    my $socket  = $self->{_socket};
    die "error: No connection\n" if !defined $socket || !$socket->connected;

    my @command = @_;

    my $argc = @command;
    if ($argc > 0) {
      my @argv = ();
      # add command argument count
      push @argv, "*$argc";
      for (my $i = 0; $i < $argc; $i = $i + 1) {
        my $v = @command[$i];
        push @argv, '$' . length($v);
        push @argv, $v;
      }
      my $buffer = join("\r\n", @argv) . "\r\n";

      my $bytes = syswrite( $socket, $buffer, length($buffer) );
      die "$!\n" unless defined $bytes;

      return $self->__readReply();
    }
    return undef;
  }

  # close the connection
  sub close {
    my ( $self ) = @_;

    defined $self->{_socket}
        and $self->{_socket}->close();
  }

  sub __readReply {
    my $self    = $_[0];
    my $socket  = $self->{_socket};

    my $data    = '';
    my $buffer  = $self->{_buffer};
    my $builder = $self->{_builder};
    while (1) {
      my $bytes = sysread( $socket, $data, BUFFER_SIZE );
      # return unless defined $bytes && $bytes;
      if (defined $bytes) {
        $buffer->append($data);
        my $ok = $builder->resolve($buffer);
        if ($ok) {
          return $builder->reply;
        }
      }
    }
    return undef;
  }
}
1;
