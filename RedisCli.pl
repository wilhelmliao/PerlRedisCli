package main;
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
      die "Unknown argument '$arg'";
    }
    else {
      unshift(@args, $arg);
      @EXEC_COMMAND = @args;
      last;
    }
    next;
  }
}

sub __execCommandArgs {
  my $redis = shift;
  my @args  = @_;

  my $reply = $redis->exec(@args);
  StdOutFormatter::print($reply);
}

# main
{
  # parse arguments
  __parse_args(@ARGV);

  my $redis;
  $redis = new Redis($HOST, $PORT);
  $redis->connect();

  # auth
  if (defined $SECRET) {
    __execCommandArgs($redis, 'AUTH', $SECRET);
  }
  # change database
  if (defined $DBNUM) {
    __execCommandArgs($redis, 'SELECT', $DBNUM);
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
  }
}
exit 0;
