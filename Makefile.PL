use inc::Module::Install;
name 'POE-Component-Drizzle';
all_from 'lib/POE/Component/Drizzle.pm';

requires 'POE'          => 1.006;
requires 'Net::Drizzle' => 0.05;

tests 't/*.t';
author_tests 'xt';

build_requires 'Test::TCP';
build_requires 'Test::SharedFork';
build_requires 'Test::More';
use_test_base;
auto_include;
WriteAll;
