#!/home/oracle/product/12.2.0.1/client/perl/bin/perl
use strict;
use warnings;

use DBI;
use DBD::Oracle;

my $DEBUG = 'Y';

my %params;


    $params{conf_table} = 'GG_TABLES';
    $params{conf_owner} = 'DW_STG';


my $gghome = '/home/oracle/product/ggate12c';

my $toDate = `date +%d-%m-%Y" "%H:%M:%S`;
chomp $toDate;
print "\n\n   *** Run GG tables parser at $toDate ***\n\n";
my $replicats = get_replicats();

my $dbh;
my $last_db_conn;
my $last_db_user;

foreach my $rec (@$replicats) {

         $dbh = DBI->connect( "dbi:Oracle:$rec->{dbname}",
                        $rec->{user},
                        "*****************",
                        { RaiseError => 1, AutoCommit => 0 }
                      ) || die "Database connection not made: $DBI::errstr";

                my $delcnt = deleteNotFoundTable($rec, $dbh);
                my $inscnt = insertNotFoundTable($rec, $dbh);
        print "** $rec->{repl_name}: deleted $delcnt merged $inscnt\n";

         $dbh->disconnect;

}




exit;


###################################################################################################


sub send_command {

    my $command = shift;

    # Do not intend this as EOF should be BOL
    my @lines = `$gghome/ggsci<<EOF
$command
EOF`;

    return \@lines;

}


sub clear_unusable_tables {
    my $dbh                 = shift;
    my $repl_name           = shift;


    my $agg_tables = table2a_repl($repl_name);



    my $sql_text = qq {
            select t.group_name, t.group_key, t.owner, t.table_name
                from bis_gg.gg_tables t where t.group_name = upper('$repl_name')
    };


    my $sth = $dbh->prepare($sql_text);
        $sth->execute();

    my($group_name, $group_key, $owner, $table_name); # Declare columns


        $sth->bind_columns(undef, \$group_name, \$group_key, \$owner, \$table_name);


    while( $sth->fetch()) {

            unless ( find_rec($agg_tables, "$owner.$table_name")){
                delete_rec($dbh, $repl_name, "$owner.$table_name");
                print "Not found -> $owner.$table_name\n";
            }

            #print("** $group_name, $group_key, $owner, $table_name\n");

    }

    $sth->finish();
}






sub delete_rec {
    my $dbh = shift;
    my $repl_name = shift;
    my $table_name = shift;

    my $rows4;

    my $sql_text = qq {
            delete from $params{conf_owner}.$params{conf_table} where group_name='$repl_name'
                and owner||'.'||table_name = '$table_name'
            };


#       print "$sql_text\n";

    unless ( $rows4 = $dbh->do("$sql_text")) {
    &programError(   "Could not do $params{conf_owner}.$params{conf_table}"
                , "$sql_text"
                , "$DBI::errstr"
                , "dba-mail"
                , "dba-pager");
    $dbh->rollback;
    }

    print "$rows4 rows deleted for $table_name replicat $repl_name\n";

    unless ($dbh->commit) {
    &programError(   "Could not commit GG_TABLES transaction"
                , "$sql_text"
                , "$DBI::errstr"
                , "dba-mail"
                , "dba-pager");
    $dbh->rollback;
    }

}




sub insert_rec {
    my $dbh         = shift;
    my $repl_name   = shift;
    my $owner       = shift;
    my $table       = shift;
    my $src_owner   = shift;
    my $src_table   = shift;
    my $extp        = shift;
    my $chkptbl     = shift;

    my $rows4;

    $extp =~ s/\'/\'\'/g;
    my $sql_text = qq {
            insert into $params{conf_owner}.$params{conf_table} (group_name, group_key, owner, table_name, src_table_owner, src_table_name, insert_date, ext_params)
                    select group_name, max(group_key), '$owner', '$table', '$src_owner', '$src_table', to_date('$toDate','dd-mm-yyyy hh24:mi:ss'), '$extp'
                        from $chkptbl where group_name = upper('$repl_name')
                        group by group_name, '$owner', '$table', '$src_owner', '$src_table', to_date('$toDate','dd-mm-yyyy hh24:mi:ss')
            };


#       print "$sql_text\n";

    unless ( $rows4 = $dbh->do("$sql_text")) {
    &programError(   "Could not do INSERT INTO $params{conf_owner}.$params{conf_table}"
                , "$sql_text"
                , "$DBI::errstr"
                , "dba-mail"
                , "dba-pager");
    $dbh->rollback;
    }

    print "$rows4 row inserted: $src_owner.$src_table > $owner.$table for replicat $repl_name\n";

    unless ($dbh->commit) {
    &programError(   "Could not commit $params{conf_owner}.$params{conf_table} transaction"
                , "$sql_text"
                , "$DBI::errstr"
                , "dba-mail"
                , "dba-pager");
    $dbh->rollback;
    }

}

sub merge_rec {
    my $dbh         = shift;
    my $repl_name   = shift;
    my $owner       = shift;
    my $table       = shift;
    my $src_owner   = shift;
    my $src_table   = shift;
    my $extp        = shift;
    my $chkptbl     = shift;
    my $rows4;

    $extp =~ s/\'/\'\'/g;
    my $sql_text = qq {
            merge into $params{conf_owner}.$params{conf_table} t
            using (
                select group_name, max(group_key) as group_key, '$owner' as tow, '$table' as tn, '$src_owner' as sto, '$src_table' as stn,
                    to_date('$toDate','dd-mm-yyyy hh24:mi:ss') as idate, '$extp' as ep
                from $chkptbl where group_name = upper('$repl_name')
                group by group_name
            ) q
            on (t.group_name = q.group_name and t.owner = q.tow and t.table_name = q.tn)
            when not matched then insert (group_name, group_key, owner, table_name, src_table_owner, src_table_name, insert_date, ext_params)
                values (q.group_name, q.group_key, q.tow, q.tn, q.sto, q.stn, q.idate, q.ep)
            when matched then update set
                t.src_table_owner = q.sto,
                t.src_table_name = q.stn,
                t.ext_params = q.ep
            };


#       print "$sql_text\n";

    unless ( $rows4 = $dbh->do("$sql_text")) {
    &programError(   "Could not do merge into $params{conf_owner}.$params{conf_table}"
                , "$sql_text"
                , "$DBI::errstr"
                , "dba-mail"
                , "dba-pager");
    $dbh->rollback;
    }

    print "$rows4 row merged: $src_owner.$src_table > $owner.$table for replicat $repl_name\n";

    unless ($dbh->commit) {
    &programError(   "Could not commit $params{conf_owner}.$params{conf_table} transaction"
                , "$sql_text"
                , "$DBI::errstr"
                , "dba-mail"
                , "dba-pager");
    $dbh->rollback;
    }
}

################################################################################
sub check_table {
################################################################################
    my $dbh = shift;
    my $repl_name = shift;
    my $owner = shift;
    my $table = shift;


    my $sql_text = qq {
            select 1 from  $params{conf_owner}.$params{conf_table} t
                where group_name = upper('$repl_name')
                and owner = upper('$owner')
                and table_name = upper('$table')
            };

    my $sth = $dbh->prepare($sql_text);
        $sth->execute();

    my($retval); # Declare columns


        $sth->bind_columns(undef, \$retval);


    $retval = 0 unless ($sth->fetch());

    $sth->finish();

    return $retval;

}



#####################################################################################
sub getTables {
#####################################################################################
    my $string  = shift;
    my $hconf   = shift;

    my $atables;

    $string =~ /MAP\s+(\w+)(\.*)([\w|\*|\$|\?|\-]*)\s*,{0,1}\s*TARGET\s+(\w+)(\.*)([\w|\*|\$|\-]+)[,|\s]*([^;]*);*/;


    my ($sown, $stbl, $town, $ttbl, $extp)  = ($1, $3, $4, $6, $7);

    if ("x$stbl" eq "x") {  #$stbl is empty
            $stbl = $sown;
            $sown = '';
    }

    if ("x$ttbl" eq "x") {  #$ttbl is empty
            $ttbl = $town;
            $town = '';
    }

    $ttbl = $stbl if $ttbl eq '*';

    print "MAP: $sown.$stbl >> $town.$ttbl, $extp \n" if $DEBUG eq 'Y';

    if ($ttbl =~ /\*/) {
            $ttbl =~ s/\*/\%/g;
    }


    $atables = getDbTablesByMask($town,$ttbl, $hconf, $sown, $stbl, $extp);


    return $atables;

}


#######################################################################################
sub getDbTablesByMask {
#
#
#######################################################################################

        my $owner = shift;
        my $table = shift;
        my $hconf = shift;
        my $src_owner = shift;
        my $src_table = shift;
        my $extp = shift;

        my $sowner = '';

        my $dbh;

        my @atables;

        my $counter = 0;


        unless ("x$owner" eq "x") {
                $sowner = "and owner = upper('$owner')";
        }

        my $sqltext = qq {
                                select owner, table_name
                                from dba_tables where table_name like upper('$table') $sowner
                                };

        unless ( defined ( $hconf->{dbh} )) {
                 $dbh = DBI->connect( "dbi:Oracle:$hconf->{dbname}",
                                $hconf->{user},
                                "a1b2c3d4",
                                { RaiseError => 1, AutoCommit => 0 }
                              ) || die "Database connection not made: $DBI::errstr";
        }
        else {

                $dbh = $hconf->{dbh};
        }

        my $sth = $dbh->prepare($sqltext);

        $sth->execute();

        my($rowner, $rtable); # Declare columns


        $sth->bind_columns(undef, \$rowner,\$rtable);

        while ($sth->fetch()) {
                push(@atables, {
                        owner   => $rowner,
                        table   => $rtable,
                        src_owner => $src_owner,
                        src_table => $src_table,
                        extp => $extp
                        });

                $counter++;

        }


        $sth->finish();

        if ( $counter eq 0 ){
                print("!!! ERROR !!!\n!!! ERROR !!! Table $owner.$table not exists in database\n!!! ERROR !!!\n");
        }

        unless ( defined ( $hconf->{dbh} )) {
                $dbh->disconnect;
        }



        return \@atables;

}



######################################################################################
sub getCheckpointTable {
######################################################################################
        my $repl_name = shift;

        my $lines = send_command("info $repl_name detail");

        while(my $string = shift @$lines){
                chomp($string);
                if ($string =~ /Checkpoint table\s+(\w+\.*\w*)\s*/) {
                        return $1;
                }
        }

        return '';
}


######################################################################################
sub get_replicats {
######################################################################################

        my $strings = send_command('Info all');

        my @replicats;

        foreach my $string (@$strings){

                if (($string =~ /^REPLICAT\s+(\w+)\s+(\w+)/) ) {
                        my $replicat    = $2;
                        my $status              = $1;

                        unless ( $status =~ /STOPPED/ ){
                                my $info = getReplicatInfo($replicat);
                                push (@replicats, $info);
#                               return \@replicats; #for test usage
                        }
                }

        }
        return \@replicats;
}



#######################################################################################
sub getReplicatInfo {
#######################################################################################
        my $repl_name = shift;

        my %repl_info;

        $repl_info{repl_name} = $repl_name;

        my $lines = send_command("info $repl_name detail");

        while(my $string = shift @$lines){
                chomp($string);
                if ($string =~ /Checkpoint table\s+(\w+\.*\w*)\s*/) {
                        $repl_info{chkp_table} = $1;
                }
        }

        print("REPL: $repl_name\n") if $DEBUG eq 'Y';

        $lines = send_command("view params $repl_name");

        my @atables;

        my $dbh;

        while(my $string = shift @$lines){

                chomp($string);
#               print "** $string\n";

#               if ($string =~ /USERID\s+(\w+)\@(\w+)\s*,\s*PASSWORD\s+(.+)\s*/) {
                if ($string =~ /^\s*(USERID|USERIDALIAS)\s*/) {
                        $repl_info{dbname} = 'dwx';
                        $repl_info{user}         = 'DW_STG';
                        $repl_info{passwd} = 'a1b2c3d4';

                        $dbh = DBI->connect( "dbi:Oracle:$repl_info{dbname}",
                                $repl_info{user},
                                $repl_info{passwd},
                                { RaiseError => 1, AutoCommit => 0 }
                                ) || die "Database connection not made: $DBI::errstr";


                        $repl_info{dbh} = $dbh;

                        print("DNAME: $repl_info{dbname}; USER: $repl_info{user}\n") if $DEBUG eq 'Y';

                }


                unless ($string =~ /^\s*--/) {     #not a comments

                        if ( $string =~ /^\s*(OBEY|obey)\s+\.{0,1}(\/.+)/){
                                my $file_name = $gghome.$2;
                                print("Got obey statement: $string, $file_name\n") if $DEBUG eq 'Y';

                                if ( -e $file_name ) {                  # Check file exists

                                        open(F,$file_name) || die "Can't open cofigure file '$file_name'\n";

                                        while (<F>){

                                $string = $_;
                                chomp($string);

                                unless ($string =~ /^\s*--/) {  #not a comments
                                        if ($string =~ /MAP\s+(.+)\s*,{0,1}\s*TARGET/) {
                                        my $a = getTables($string, \%repl_info);
                                                        push (@atables, @$a);

                                                }

                                                }
                                }

                                        $string = '';

                                        close(F);
                                }

                        }

                   if ($string =~ /MAP\s+(.+)\s*,{0,1}\s*TARGET/) {
                         my $a = getTables($string, \%repl_info);
                         push (@atables, @$a);

                }
                }
        }

        if (defined ($repl_info{dbh})){
          $repl_info{dbh}->disconnect;
        }

        $repl_info{tables} = \@atables;

        return \%repl_info;

}


###################################################################################################
sub deleteNotFoundTable {
###################################################################################################
        my $hrepl       = shift;
        my $dbh         = shift;

        my $sqltext = qq {
                                select owner, table_name
                                from $params{conf_owner}.$params{conf_table} where group_name like '$hrepl->{repl_name}'
                                };



        my $sth = $dbh->prepare($sqltext);

        $sth->execute();

        my($rowner, $rtable); # Declare columns

        my $counter = 0;

        $sth->bind_columns(undef, \$rowner,\$rtable);


        while( $sth->fetch()) {

                if (findTableInArray($rowner, $rtable, $hrepl->{tables}) == 0) {
                    #delete table
                         delete_rec($dbh, $hrepl->{repl_name} ,"$rowner.$rtable");
                         print("* delete -> $rowner.$rtable\n") if ($DEBUG eq 'Y');
                         $counter++;
                }
#               else {
#                       print("* find ok -> $rowner.$rtable\n") if ($DEBUG eq 'Y');
#               }

        }

        $sth->finish();

        return $counter;
}


###############################################################################
sub findTableInArray {
###############################################################################
        my $owner = shift;
        my $table = shift;
        my $atables = shift;

        foreach my $rec (@$atables){
                if ( (uc($rec->{owner}) eq uc($owner)) && (uc($rec->{table}) eq uc($table ))) {
                   return 1;
                }
        }
        return 0;
}


################################################################################
sub insertNotFoundTable {
################################################################################
        my $hrepl       = shift;
        my $dbh         = shift;

        my $atables = $hrepl->{tables};
        my $counter = 0;

        foreach my $key (@$atables) {
           #if ( check_table($dbh, $hrepl->{repl_name}, $key->{owner}, $key->{table}) eq 0 ){
            #                   insert_rec($dbh, $hrepl->{repl_name}, $key->{owner}, $key->{table}, $key->{src_owner}, $key->{src_table}, $key->{extp}, $hrepl->{chkp_table});
            #   print "Not found: $key->{owner}.$key->{table} for $hrepl->{repl_name}\n";
            #   $counter++;
            #   }
            merge_rec ($dbh, $hrepl->{repl_name}, $key->{owner}, $key->{table}, $key->{src_owner}, $key->{src_table}, $key->{extp}, $hrepl->{chkp_table});
            $counter++;
        }
        return $counter;
}
