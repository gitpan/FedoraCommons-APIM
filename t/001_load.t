# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 13;
use SOAP::Lite;
use MIME::Base64;
use POSIX;

BEGIN { use_ok( 'FedoraCommons::APIM' ); }

my $host = $ENV{FEDORA_HOST} || "";
my $port = $ENV{FEDORA_PORT} || "";
my $user = $ENV{FEDORA_USER} || "";
my $pwd  = $ENV{FEDORA_PWD} || "";

my $skip_deletions = 1;
my @temp_files = ();


#1 Check Fedora server arguments
ok ($host && $port && $user && $pwd, 
    'Fedora server Environment variables not set');

if (! $host || ! $port || ! $user || ! $pwd) {
    my $msg = "Fedora server environment variables not set properly.";
    diag ("$msg");
    BAIL_OUT ("$msg");
} 

diag ("Host: $host Port: $port User: $user Pwd: $pwd\n");

my $timeout = 100;

# Create APIM Object
my $apim = new FedoraCommons::APIM
    ( host => $host, 
      port => $port, 
      usr  => $user, 
      pwd  => $pwd, 
      timeout => $timeout, 
      replace => 1, 
      reload => 0,
      debug => 1,
  );

my $error = $apim->error() || "";
my $ref   = ref $apim || "";

#2 Check APIM object
isa_ok ($apim, 'FedoraCommons::APIM');

if (ref ($apim) ne 'FedoraCommons::APIM') {
    my $msg = "Unable to instantiate APIM object properly. Error: $error";
    diag ("$msg");
    BAIL_OUT ("$msg");
}

#3
ok ($apim, "Failed to instantiate APIM object.");

# Test createObject
my $pid = "CPAN:TestObject";
my $collection = "CPAN Test Collection";

my $result = $apim->createObject 
    (
     XML_file=> "./ingesttemplate.xml",
     params => {pid_in => $pid,
		title_in => "Test object: $collection",
		collection_in => "CPAN:Test"},
     pid_ref =>\$pid);

$error = $apim->error() || "";
#4
ok ($result == 0, "Fedora createObject() FAILED: $error");

my $pid2 = "CPAN:TestObject2";

SKIP: {
    skip "Create Object Failed", 6 if $result != 0;

    # Let's create file to upload as datastream
    my $data = $apim->get_default_foxml();
    my $testFile = POSIX::tmpnam() . 'testFile';
    open my $fh, ">", $testFile;
    if (defined $fh) {
	binmode $fh, ":utf8";
	print $fh $testFile;
	close $fh;
    }
    push @temp_files, $testFile;

    # Test uploadNewDatastream (addDatastream, setDatastreamVersionable)
    #     Relies on test object, test several methods
    my $mime = "text/xml";
    my ($dsid, $ts, $dsLabel);
    my $dsID = "TEST_DATASTREAM";
    $dsLabel = "Test Datastream";
    $ts = "";
    $result = $apim->uploadNewDatastream( pid => $pid,
					  dsID => $dsID,
					  filename => $testFile,
					  MIMEType => $mime,
					  dsid_ref => \$dsid,
					  timestamp_ref => \$ts,
					  );
    $error = $apim->error() || "";
    diag ("uploadNewDatastream timestamp: $ts");
    # A
    ok ($result == 0, "Fedora uploadNewDatastream() FAILED: $error");

    # Test compareDatastreamChecksum
    #     Relies on test object
    my $checksum_result;
    $dsID = "TEST_DATASTREAM";
    my $versionDate = undef;
    $result = $apim->compareDatastreamChecksum 
	( pid => $pid,
	  dsID => $dsID,
	  versionDate => $versionDate,
	  checksum_result =>\$checksum_result,
	  );
    $error = $apim->error() || "";
    # B
    ok ($result == 0, "Fedora compareDatastreamChecksum() FAILED: $error");

    diag ("Checksum: $checksum_result");

    # Test getDatastream
    #     Relies on test object
    my $stream;
    $result = $apim->getDatastream 
	( pid => $pid,
	  dsID => $dsID,
	  ds_ref => \$stream,
	  );
    $error = $apim->error() || "";
    # C
    ok ($result == 0, "Fedora getDatastream() FAILED: $error");

    diag ("Datastream: $stream");

    # Test addRelationship 
    #     Relies on test object
    my $base = "info:fedora/fedora-system:def/relations-external#";
    my $relation = "$base"."isMetadataFor";
    $result = $apim->addRelationship( pid => $pid,
				      relationship => $relation,
				      object => "$pid2",
				      isLiteral => 'false',
				      datatype => "",
				      );

    # D
    ok ($result == 0, "Fedora addRelationship() FAILED: $error");

    # Test purgeDatastream
    #     Relies on test object
  SKIP: {
      skip "Deletions disabled: purgeDatastream()", 1 
	  if ($skip_deletions == 1);
      my $force = 0;
      my ($startDT, $endDT, $ts);
      $result = $apim->purgeDatastream
	  (pid =>  $pid,
	   dsID => $dsID,
	   startDT => $startDT,
	   endDT => $endDT,
	   logMessage => "Test of purgeDatastream()",
	   timestamp_ref => \$ts,
	   );

      # E
      ok ($result == 0, "Fedora purgeDatastream() FAILED: $error");
  }
    # Test purgeRelationship
    #     Relies on test object
  SKIP: { 
      skip "Deletions disabled: purgeRelationship()", 1 
	  if ($skip_deletions == 1);
      my $return;
      $result = $apim->purgeRelationship( pid => $pid,
					  relationship => $relation,
					  object => "$pid2",
					  isLiteral => 'false',
					  datatype => "",
					  result => \$return,
					  );
      
      # F
      ok ($result == 0, "Fedora purgeRelationship() FAILED: $error");
  }

    # Test purgeObject - clear out test objects
    #     Relies on test object
  SKIP: { 
      skip "Deletions disabled: purgeRelationship()", 1 
	  if ($skip_deletions == 1);
      my $timestamp_ref;

      $result = $apim->purgeObject (pid => $pid,
				    logMessage => "Purge Test Object",
				    force => "",
				    timestamp_ref => \$timestamp_ref,
				    );

      # G
      ok ($result == 0, "Fedora purgeObject FAILED: $error");
  }
}

# Tests that do not require test object

# Test getNextPID - standalone


ok ($result == 0, "Fedora getNextPID() FAILED: $error");
