package POE::Component::Drizzle;
use strict;
use warnings;
use base qw/Exporter/;
use Net::Drizzle ':constants';
our $VERSION = '0.01';
our @EXPORT = @Net::Drizzle::EXPORT_OK;
use POE;
use IO::Poll qw/POLLIN POLLOUT/;
use 5.00801;
# use Smart::Comments;

sub spawn {
    my ($class, %args) = @_;

    my $alias = delete $args{alias};
    my $drizzle = Net::Drizzle->new()
                              ->add_options(DRIZZLE_NON_BLOCKING);

    ### create session
    POE::Session->create(
        heap => {
            drizzle      => $drizzle,
              queries    => 0,
              'shutdown' => 0,
              con_opts => \%args,
        },
        inline_states => {
            _start => sub {
                $_[KERNEL]->alias_set($alias) if $alias;
            },
            'do' => sub {
                my $args = $_[ARG0];
                ### do event: $args
                my $drizzle = $_[HEAP]->{drizzle};
                my $sql  = $args->{sql};

                my %c = %{$_[HEAP]->{con_opts}};
                ### foo: %c;
                my $newcon = $drizzle->con_add_tcp(
                                           $c{hostname} || DRIZZLE_DEFAULT_TCP_HOST,
                                           $c{port}     || DRIZZLE_DEFAULT_TCP_PORT,
                                           $c{user}     || DRIZZLE_DEFAULT_USER,
                                           $c{password} || undef,
                                           $c{database} || undef,
                                           $c{options}  || 0,
                                     );
                $newcon->query_add($sql);
                $_[HEAP]->{queries}++;

                my $container = {
                    con      => $newcon,
                    session  => $args->{session} || $_[SENDER]->ID(),
                    event    => $args->{event},
                };
                while (1) {
                    my $ret = handle_once($_[KERNEL], $_[SESSION], $_[HEAP], $container, undef);
                    if ($ret == DRIZZLE_RETURN_IO_WAIT) {
                        last;
                    }
                }
                ### select(2): $newcon->fd
                $container->{fh} = $newcon->fh;
                $_[KERNEL]->select($container->{fh}, 'handle_select', 'handle_select', undef, $container);
            },
            handle_select => sub {
                my ($mode, $container) = ($_[ARG1], $_[ARG2]);
                return unless defined $container;
                return unless defined $container->{con};

                # DEBUG("handle_select $mode, $container");
                handle_once($_[KERNEL], $_[SESSION], $_[HEAP], $container, $mode);
            },
            'shutdown' => sub {
                $_[HEAP]->{'shutdown'}++;
                ### SHUTDOWN event. remains: $_[HEAP]->{queries}
                if ($_[HEAP]->{queries} == 0) {
                    ### shutdown me
                    if ($alias) {
                        $_[KERNEL]->alias_remove($alias) if $alias;
                        undef $_[HEAP]->{drizzle};
                    }
                }
            },
            _stop => sub {
                ### stop event occured
            },
        },
    );
}

sub handle_once {
    my ($kernel, $session, $heap, $container, $mode) = @_;
    # ## handle once

    my $drizzle = $container->{con}->drizzle;
    if (defined $mode) {
        $container->{con}->set_revents( $mode == 0 ? POLLIN : POLLOUT );
    }
    my ($ret, $query) = $drizzle->query_run();
    if ($ret != DRIZZLE_RETURN_IO_WAIT && $ret != DRIZZLE_RETURN_OK) {
        die "query error: " . $drizzle->error(). '('.$drizzle->error_code .')';
    }
    if ($query) {
        ### got a query
        my $result = $query->result;
        $kernel->select($container->{fh});
        my ($event, $session) = ($container->{event}, $container->{session});
        ### session: $session
        if (defined $event && defined $session) {
            ### got a callback! : $event, $session
            # DEBUG2("event TO $event, $session");
            $kernel->post($session, $event, $query->result);
        }
        $heap->{queries}--;
        if ($heap->{shutdown} && $heap->{queries} == 0) {
            ### SEND SHUTDOWN SIGNAL!
            $kernel->post( $session, 'shutdown' );
        }
        # $query->con->close;
        undef $container->{con};
        undef $container;
        # msg("finished query !!");
        ### finished query. remains: $heap->{queries}
    }
    return $ret;
}

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
        database => 'test',
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

POE::Component::Drizzle is a POE bindings for libdrizzle with Net::Drizzle.
You can send the queries with non-blocking I/O.

=head1 AUTHOR

Tokuhiro Matsuno E<lt>tokuhirom  slkjfd gmail.comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
