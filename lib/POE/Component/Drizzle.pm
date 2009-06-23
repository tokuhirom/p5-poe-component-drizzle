package POE::Component::Drizzle;

use strict;
use warnings;
our $VERSION = '0.01';

1;
__END__

=head1 NAME

POE::Component::Drizzle - asynchronous non-blocking mysql/drizzle calls in POE

=head1 SYNOPSIS

    use POE::Component::Drizzle;

    # set up libdrizzle session
    POE::Component::Drizzle->spawn(
        alias => 'drizzle',
        hostname => 'localhost',
        port     => 1919,
        username => 'user',
        password => 'pass',
        options  => DRIZZLE_CON_MYSQL,
    );

    # create our own session to communicate with libdrizzle
    POE::Session->create(
        inline_states => {
            _start => sub {
                $_[KERNEL]->post('drizzle',
                    do => {
                        sql => 'create table users (id int, username varchar(100))',
                        event => 'table_created',
                    },
                );
            },
            table_ceated => sub {
                my @sql = (
                    'insert users into (1, "jkondo")',
                    'insert users into (2, "reikon")',
                    'insert users into (3, "nagayama")',
                );
                for (@sql) {
                    $_[KERNEL]->post('drizzle',
                        do => { sql => $_ }
                    );
                }
                # This will let the existing queries finish, then shutdown
                $_[KERNEL]->post('drizzle', 'shutdown');
            },
        },
    );

=head1 DESCRIPTION

POE::Component::Drizzle is a bindings for libdrizzle with Net::Drizzle.
You can send the queries with non-blocking I/O.

=head1 AUTHOR

Tokuhiro Matsuno E<lt>tokuhirom  slkjfd gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
