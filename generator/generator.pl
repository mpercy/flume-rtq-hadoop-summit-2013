#!/usr/bin/perl
####################################################
# Generates output of the following format:
# txnid,ts,action,prod_id,user_ip,user_id,path,referer
# 1,123456789,view,1130,255.255.255.255,mpercy,/product/1130,/product/1101
####################################################

use strict;
use warnings;

use Time::HiRes ();

my @actions = qw(view view view view view view view view view view click click click click click buy);
my @prod_ids = (10000 .. 92000);
my @users = ();
open(USERS, "< ./users.txt") or die "$!";
chomp(@users = <USERS>);
close USERS;

my $txnId = time() * 1000 + 1;

# randomly select one element of the referenced array
sub randsel(\@) {
  my $arr = shift or die;
  ref $arr eq 'ARRAY' or die;
  my $num = scalar @$arr;
  #print STDOUT "NUM = $num\n";
  my $idx = int(rand($num));
  #print STDOUT "IDX = $idx\n";
  my $val = $arr->[$idx];
  #print STDOUT "VAL = $val\n";
  return $val;
}

sub rand_ip() {
  return int(rand(254)+1) . "." . int(rand(255)) . "." . int(rand(255)) . "." . int(rand(254)+1);
}

while (1) {
  my $tx = $txnId++;
  my $t = int(Time::HiRes::time() * 1000);
  my $a = randsel(@actions);
  my $p = randsel(@prod_ids);
  my $ip = rand_ip();
  my $u = randsel(@users);
  my $cur_prod = randsel(@prod_ids);
  if ($a eq 'buy') { $cur_prod = $p; }
  my $url = "/product/${cur_prod}";
  my $ref = '/product/' . randsel(@prod_ids);
  if ($a eq 'view') { if (rand(10) > 7) { $ref = ''; } }
  print STDOUT join(',', $tx, $t, $a, $p, $ip, $u, $url, $ref), "\n";
}
