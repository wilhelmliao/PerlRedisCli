#!/usr/bin/perl
package main;
use strict;
use warnings;
use Data::Dumper;
use Redis;

# print Dumper(\@ARGV);

my $HOST    = undef;
my $PORT    = undef;
my $DBNUM   = undef;
my $SECRET  = undef;
my @EXEC_COMMAND;
my $LOAD_SCRIPT;


sub __parse_args {
  my @args = @_;

  while (1) {
    my $arg = shift @args;
    last unless defined $arg;

    if    ($arg eq '-h') { $HOST   = shift @args; }
    elsif ($arg eq '-p') { $PORT   = shift @args; }
    elsif ($arg eq '-n') { $DBNUM  = shift @args; }
    elsif ($arg eq '-a') { $SECRET = shift @args; }
    elsif ($arg eq '--register-script') {
      $LOAD_SCRIPT = shift @args;
    }
    elsif ($arg =~ /^-/) {
      die "[ERROR] Unknown argument '$arg'";
    }
    else {
      unshift(@args, $arg);
      @EXEC_COMMAND = @args;
      last;
    }
    next;
  }
}

sub __printReply {
  my $reply = shift;

  StdOutFormatter::print($reply);
}

# NOte: sds *sdssplitargs()
# see https://github.com/antirez/sds/blob/78df7252764566d9fd8b2fbccf6a320e77f3026e/sds.c#L959
sub __split_args {
  my $input = $_[0];

  my @args = ();
  for (my $i = 0; $i < length($input); $i++) {
    my $c = substr($input, $i, 1);
    next if ($c =~ /[[:space:]]/);

    my $quoted_sign = undef;
    my $token       = undef;

    for (;; $i++) {
      if ($i < length($input)) {
        $c = substr($input, $i, 1);
      } else {
        undef $c;
      }
      if ((defined $quoted_sign) && ($quoted_sign eq '"')) {
        if (!defined $c) {
          goto err;
        }
        $token = $token || '';
        if ($c eq "\\") {
          my $escape_char = substr($input, ++$i, 1);
          if (!defined $escape_char) {
            goto err;
          }
          $c = $escape_char;
          if ($c eq 'x') {
            my $hex = substr($input, $i + 1, 2);
            if ((defined $hex) && ($hex !~ /[0-9a-f]{2}/i)) {
              $token = $token . pack('H*', $hex);
              $i = $i + 2;
            } else {
              $token = $token . $c;
            }
          }
          elsif ($c eq 'n') { $token = $token . "\n"; }
          elsif ($c eq 'r') { $token = $token . "\r"; }
          elsif ($c eq 't') { $token = $token . "\t"; }
          elsif ($c eq 'b') { $token = $token . "\b"; }
          elsif ($c eq 'a') { $token = $token . "\a"; }
          else {
            $token = $token . $c;
          }
        } elsif ($c eq '"') {
          # closing quote must be followed by a space or
          # nothing at all.
          if ((++$i) < length($input)) {
            $c = substr($input, $i, 1);
            if ((defined $c) && ($c !~ /[[:space:]]/)) {
              goto err;
            }
          }
          push @args, ($token || '');
          undef $token;
          undef $quoted_sign;
        } else {
          $token = $token . $c;
        }
      } elsif ((defined $quoted_sign) && ($quoted_sign eq "'")) {
        if (!defined $c) {
          goto err;
        }
        $token = $token || '';
        if ($c eq "\\") {
          my $next_char = substr($input, $i + 1, 1);
          if ((defined $next_char) && ($next_char eq "'")) {
            $i++;
            $token = $token . $next_char;
          } else {
            $token = $token . $c;
          }
        } elsif ($c eq "'") {
          # closing quote must be followed by a space or
          # nothing at all.
          if ((++$i) < length($input)) {
            $c = substr($input, $i, 1);
            if ((defined $c) && ($c !~ /[[:space:]]/)) {
              goto err;
            }
          }
          push @args, $token;
          undef $token;
          undef $quoted_sign;
        } else {
          $token = $token . $c;
        }
      } else {
        last unless defined $c;

        if ($c =~ /[ \n\r\t\0]/) {
          if (defined $token) {
            push @args, $token;
            undef $token;
          }
        } elsif ($c =~ /['"]/) {
          $quoted_sign = $c;
        } else {
          $token = ($token || '') . $c;
        }
      }
    }
    if (defined $token) {
      push @args, $token;
    }
  }
  return \@args;

  err:
  return undef;
}


sub sendCommandArgs {
  my $redis = shift;
  my @args  = @_;

  my $reply = $redis->exec(@args);
  if (defined $reply) {
    if (($args[0] =~ /^select$/i) && (scalar @args == 2) && ($reply->type == REDIS_REPLY_STATUS)) {
      $DBNUM = int($args[1]);
    }
  }
  return $reply;
}


# main
{
  # parse arguments
  __parse_args(@ARGV);

  my $redis;
  $redis = new Redis($HOST, $PORT);
  $redis->connect()
      or die "$!\n";

  $HOST = $redis->host;
  $PORT = $redis->port;

  my $authenticated = undef;
  # auth
  if (defined $SECRET) {
    my $reply = sendCommandArgs($redis, 'AUTH', $SECRET);
    if (defined $reply) {
      if ($reply->type eq REDIS_REPLY_STATUS) {
        $authenticated = 1;
      } else {
        my $checkReply = sendCommandArgs($redis, 'PING');
        if (defined $checkReply  &&  $checkReply->type == REDIS_REPLY_STATUS) {
          $authenticated = 1;
        } else {
          __printReply($reply);
        }
      }
    }
  }
  # change database
  if (defined $DBNUM) {
    if (defined $authenticated  &&  $authenticated eq 1) {
      my $reply = sendCommandArgs($redis, 'SELECT', $DBNUM);
      if (defined $reply  &&  $reply->type ne REDIS_REPLY_STATUS) {
        __printReply($reply);
        undef $DBNUM;
      }
    } else {
      undef $DBNUM;
    }
  }
  # register script
  if (defined $LOAD_SCRIPT) {
    my $script;
    {
      open(FH, '<', $LOAD_SCRIPT) or die $!;
      $script = do {local $/; <FH>};
      close(FH);
    }
    __execCommandArgs($redis, 'SCRIPT', 'LOAD', $script);
    exit 0;
  }
  # execute command
  if (@EXEC_COMMAND) {
    __execCommandArgs($redis, @EXEC_COMMAND);
    exit 0;
  }

  until (0) {
    print "$HOST:$PORT";
    if (defined $DBNUM  &&  $DBNUM != 0) {
      print "\[$DBNUM\]";
    }
    print "> ";

    my $input = <STDIN>;
    chomp ($input);
    my $argv = __split_args($input);
    unless (defined $argv) {
      print "[ERROR] Invalid argument(s)\n";
      next;
    }
    my @commandArgs = @$argv;
    if (@commandArgs) {
      my $commandArgs = @commandArgs;
      if ((scalar @commandArgs == 1) && ($commandArgs[0] =~ /exit/i)) {
        last;
      }
      my $reply = sendCommandArgs($redis, @commandArgs);
      __printReply($reply);
    }
  }
}
exit 0;
