use strict;
use warnings;
use Test::TCP;
sub POE::Kernel::ASSERT_DEFAULT () { 1 }
use POE qw/
    Component::Drizzle
    Session
/;
# use Smart::Comments;
use Test::More tests => 4;
use POSIX ":sys_wait_h";

sub true  () { 1 }
sub false () { 0 }

test_tcp(
    client => \&run_client,
    server => \&run_server, 
);
exit;

sub run_client {
    my $port = shift;

    # set up libdrizzle session
    POE::Component::Drizzle->spawn(
        alias => 'drizzle',
        hostname => 'localhost',
        port     => $port,
        username => 'user',
        password => 'pass',
        database => 'test',
        options  => DRIZZLE_CON_MYSQL,
    );

    # create session for your own
    my $queries = 4;
    POE::Session->create(
        inline_states => {
            _start => sub {
                $_[KERNEL]->post('drizzle',
                    do => {
                        sql => 'create table users (id int, username varchar(100))',
                        event => 'table_created',
                    },
                );
                $_[KERNEL]->alias_set('foo');
            },
            _default => sub {
                fail "unhandled event: " . $_[ARG0];
            },
            table_created => sub {
                $queries--;
                ### table created event
                my @sql = (
                    'insert users into (1, "jkondo")',
                    'insert users into (2, "reikon")',
                    'insert users into (3, "nagayama")',
                );
                for (@sql) {
                    $_[KERNEL]->post('drizzle',
                        do => {
                            sql => $_,
                            event => 'inserted',
                        }
                    );
                }
            },
            inserted => sub {
                ### inserted
                $queries--;
                if ($queries == 0) {
                    $_[KERNEL]->post('drizzle',
                        do => {
                            sql => 'SHUTDOWN',
                            event => 'server_shutdown_succeeded',
                        }
                    );
                }
            },
            server_shutdown_succeeded => sub {
                # This will let the existing queries finish, then shutdown
                $_[KERNEL]->post('drizzle', 'shutdown');
                $_[KERNEL]->post($_[SESSION], 'shutdown');
                $_[KERNEL]->post($_[KERNEL], 'shutdown');
                ok 1, "succeded the process";
            },
            'shutdown' => sub {
                ### stop the kernel
                ok 1, 'shutdown event';
            },
            _stop => sub {
                until ((my $pid = waitpid( -1, 0 )) == -1) {
                    ### nop: $pid
                }
                ok 1, "stop the process!";
            },
        },
    );

    POE::Kernel->run;
    ok 1, "terminated poe";
}

# -------------------------------------------------------------------------
my $MASTER_PID = 0;
my $SHUTDOWN_SERVER = 0;

sub run_server {
    my $port = shift;
    my $sock = IO::Socket::INET->new(
        LocalAddr => '0.0.0.0',
        LocalPort => $port,
        Proto     => 'tcp',
        ReuseAddr => 1,
        Listen    => 10,
    ) or die $!;
    my $I_AM_PARENT = 1;
    my %children;
    local $SIG{PIPE} = sub { warn "SIGPIPE" };
    local $SIG{CHLD} = sub { wait };
    local $SIG{TERM} = sub {
        if ($I_AM_PARENT == 1) {
            kill 9, (keys %children);
        }
        my $kid;
        do {
            $kid = waitpid( -1, WNOHANG );
        } while $kid > 0;
        exit;
    };
    $MASTER_PID = $$;
    my $drizzle = Net::Drizzle->new();
    while (1) {
        my $csock = $sock->accept or next;
        ### ACCEPT! : $csock

        my $pid = fork();
        if ($pid == 0) {
            local $SIG{TERM} = sub { exit; };
            $I_AM_PARENT = 0;
            my $con = $drizzle->con_create()
                            ->set_fd($csock->fileno)
                            ->add_options(Net::Drizzle::DRIZZLE_CON_MYSQL);
            handle_con($con);
            if ($SHUTDOWN_SERVER) {
                kill 15, $MASTER_PID;
            }
            exit;
        } else {
            $children{$pid} = 1;
        }
    }
}

sub handle_con {
    my $con = shift;

    # Handshake packets.
    $con->set_protocol_version(10)
        ->set_server_version("Net::Drizzle example 1.2.3")
        ->set_thread_id(1)
        ->set_scramble("ABCDEFGHIJKLMNOPQRST")
        ->set_capabilities(Net::Drizzle::DRIZZLE_CAPABILITIES_NONE)
        ->set_charset(8)
        ->set_status(DRIZZLE_CON_STATUS_NONE)
        ->set_max_packet_size(DRIZZLE_MAX_PACKET_SIZE);

    $con->server_handshake_write();
    my $ret = $con->client_handshake_read();
    if ($ret == DRIZZLE_RETURN_LOST_CONNECTION) {
        ### LOST CONNECTION
        return;
    }

    $con->result_create()
        ->write(true);

    my $DRIZZLE_FIELD_MAX = 2;
    my $ROWS = 4;

    while (1) {
        ### command buffer
        my ($data, $command, $total, $ret) = $con->command_buffer();
        if ($ret == DRIZZLE_RETURN_LOST_CONNECTION) {
            warn "LOST CONNECTION, $ret";
            return;
        }
        if ($ret == DRIZZLE_RETURN_OK && $command == DRIZZLE_COMMAND_QUIT) {
            warn "USER QUIT";
            return;
        }
        if ($data eq 'SHUTDOWN') {
            ### SHUTDOWN COMMAND!
            $SHUTDOWN_SERVER++;
        }
        ### "got query %u '%s'\n", $command, defined($data) ? $data : '(undef)');

        my $res = $con->result_create();
        if ($command != DRIZZLE_COMMAND_QUERY) {
            ### not a query
            $res->write(true);
            warn "not a query, skipped, $command";
            next;
        }

        $res->set_column_count(2)
            ->write(false);

        $res->column_create()
            ->set_catalog("default")
            ->set_db("drizzle_test_db")
            ->set_table("drizzle_set_table")
            ->set_orig_table("drizzle_test_table")
            ->set_name("test_column_1")
            ->set_orig_name("test_column_1")
            ->set_charset(8)
            ->set_size($DRIZZLE_FIELD_MAX)
            ->set_type(DRIZZLE_COLUMN_TYPE_VARCHAR)
            ->write()
            ->set_name("test_column_2")
            ->set_orig_name("test_column_2")
            ->write();

        $res->set_eof(true)
            ->write(false);

        for my $x (1..$ROWS) {
            my @field = ("field $x-1", "field $x-2");
            $res->calc_row_size(@field) # This is needed for MySQL and old Drizzle protocol.
                ->row_write();
            $res->fields_write(@field);
        }
        $res->write(true);
        $con->close();
        return;
    }
}

