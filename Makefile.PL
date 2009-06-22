use inc::Module::Install;
name 'POE-Component-Drizzle';
all_from 'lib/POE/Component/Drizzle.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

build_requires 'Test::More';
use_test_base;
auto_include;
WriteAll;
