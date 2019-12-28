#!/usr/bin/perl
package main;
use strict;
use warnings;
use Data::Dumper;
use Term::ReadKey;
use Redis;
use Module::Load;

use Term::ANSIColor qw(:constants);
# print $^O . "\n";
if ($^O eq 'MSWin32') {
  load 'Win32::Console::ANSI';
}

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

  StdOutReplyFormatter::print($reply);
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

  $| = 1;
  until (0) {
    print "$HOST:$PORT";
    if (defined $DBNUM  &&  $DBNUM != 0) {
      print "\[$DBNUM\]";
    }
    print "> ";

    ReadMode('raw');
    my $input = '';
    while (defined(my $key = ReadKey(0))){
      if ($key =~ /[\r\n]/) {
        print "\n";
        last;
      } else {
        my $keycode = ord($key);
        if ($keycode == 8 || $keycode == 127) {
          # handle earse char
          if (length($input) > 0) {
            $input = substr($input, 0, -1);
            print "\b \b";
            # print BRIGHT_BLACK." [Hint]".RESET;
            # print "\b" x 7;
          }
        } elsif ($keycode == 9) {

        } elsif ($keycode == 27) {
          my $ctrl_sequence = '';
          while (defined(my $k = ReadKey(0))) {
            $ctrl_sequence = $ctrl_sequence . $k;
            if (length($ctrl_sequence) >= 2) {
              if ($ctrl_sequence eq '[C') {
                print chr(27).$ctrl_sequence;
              } elsif ($ctrl_sequence eq '[D') {
                print chr(27).$ctrl_sequence;
              }
              last;
            }
          }
        } else {
          $input = $input . $key;
          print $key;
          # print ord($key);
        }
        # earse 8
        # up    27,91,65
        # down  27,91,66
        # right 27,91,67
        # left  27,91,68
        # print "got key '$key'\n";

      }
    }
    ReadMode('normal');

    # my $input = <STDIN>;
    chomp ($input);
    my $argv = __split_args($input);
    unless (defined $argv) {
      print "[ERROR] Invalid argument(s)\n";
      next;
    }
    my @commandArgs = @$argv;
    if (@commandArgs) {
      my $commandArgs = @commandArgs;
      if ((scalar @commandArgs == 1) && ($commandArgs[0] =~ /^exit$/i)) {
        last;
      }
      my $reply = sendCommandArgs($redis, @commandArgs);
      __printReply($reply);
    }
  }
}
exit 0;
