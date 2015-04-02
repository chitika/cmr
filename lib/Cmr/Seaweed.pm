#
#   Copyright (C) 2014 Chitika Inc.
#
#   This file is a part of Cmr
#
#   Cmr is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

package Cmr::Seaweed;

use strict;
use warnings;

use Carp qw/cluck/;
use Redis ();
use Furl ();
use Cmr::JsonUtils ();
use Data::UUID ();
use HTTP::Status qw(:constants);

our $quick_meta  = qr/[\|\(\)\[\{\^\$\*\+\?\.]/o;
our $match_meta  = qr/(?<!\\)[\|\(\)\[\{\^\$\*\+\?\.]/o;
our $cap_meta    = qr/(?<!\\)([\|\(\)\[\{\^\$\*\+\?\.])/o;

our $CRLF = "\015\012";

sub new {
    my (%args) = @_;
    my $self = {};

    my $ug = Data::UUID->new;    

    for my $arg ('redis_addr', 'seaweed_addr') {
        $self->{$arg} = $args{$arg} or die "Missing required argument $arg\n";
    }
    
    $self->{redis} = Redis->new(server => $self->{redis_addr});
    $self->{furl}  = Furl->new(headers => ['Accept-Encoding' => 'gzip', 'Connection' => 'close']);
    $self->{uuid}  = $ug->create_str();

    $self->{db} = $args{db} // 0;
    $self->{redis}->select($self->{db});

    $self->{prefix} = $args{prefix} // "warehouse";
    
    bless $self;
}

sub __hacky_multipart {
    my ($self, $location, $data) = @_;

    # Because HTTP::Request::Common is quite slow...

    my ($filename) = $location =~ /([^\/]*)$/o;
    $filename //= "unknown";

    my $boundary = $self->{uuid};
    my $content = qq(--${boundary}${CRLF}Content-Disposition: form-data; name="file"; filename="${filename}"${CRLF}${CRLF}${data}${CRLF}--${boundary}--${CRLF});
    my $headers = HTTP::Headers->new;

    $headers->header('Content-Type'   => "multipart/form-data; boundary=${boundary}");
    $headers->header('content-length' => length($content));

    my $hack = {
        '_content' => $content,
        '_uri'     => URI->new($location, 'http:'),
        '_headers' => $headers,
        '_method'  => 'POST',
    };

    return bless ($hack, 'HTTP::Request');
}

sub set_from_file {
    my ($self, $key, $file) = @_;

    $self->{error} = 0;

    my $redis = $self->{redis};
    my $furl  = $self->{furl}; 


    $key = $self->__preprocess($key);

    my $collection = $self->__get_collection($key);
    my @features = split(/\//, $key);

    my $store_key = shift(@features);

    while(1) {
        my $sub_key = shift(@features);

        if (@features) {
            # Not a leaf, build up filesystem structure
            $redis->hset($store_key, $sub_key, 1);
            $store_key .= "/${sub_key}";
            next;
        }

        # This is a leaf... a file to be stored in the warehouse

        # Create a new file in the collection
        my $res;

        $res = $furl->get("$self->{seaweed_addr}/dir/assign?collection=${collection}");
        return $self->__http_error($res, "failed to assign collection: $collection") unless ($res->code == &HTTP_OK);

        my $loc_info = &Cmr::JsonUtils::Decode($res->decoded_content()) or die "Failed to decode assignment";
        my $location = "http://$loc_info->{'url'}/$loc_info->{'fid'}";

        # Get the size of the file we're going to post
        my $size = -s $file;
        unless (defined $size) {
            cluck "Failed to determine file size: $file\n";
            return undef;
        }

        # Post the file
        my $rc = system("curl -f -s -F file=\@${file} ${location} > /dev/null");
        unless ($rc == 0) {
            cluck "Failed to upload file";
            return undef;
        }

        # Update the index
        $redis->hset($store_key, $sub_key, "${size}:$loc_info->{'fid'}");
        last;
    }

    return 1;
} 

sub assign_dir {
    my ($self, $key) = @_;

    my $redis = $self->{redis};
    my $furl  = $self->{furl};

    $key = $self->__preprocess($key);

    my $collection = $self->__get_collection($key);

    my $res = $furl->get("$self->{seaweed_addr}/dir/assign?collection=${collection}");
    return $self->__http_error($res, "failed to assign collection: $collection") unless ($res->code == &HTTP_OK);

    my $loc_info = &Cmr::JsonUtils::Decode($res->decoded_content()) or die "Failed to decode assignment";

    return $loc_info;
}

sub _set_index {
    my ($self, $size, $key, $fid) = @_;

    $self->{error} = 0;

    my $redis = $self->{redis};
    my $furl  = $self->{furl};
    
    $key = $self->__preprocess($key);

    my $collection = $self->__get_collection($key);
    my @features = split(/\//, $key);

    my $store_key = shift(@features);

    while(1) {
        my $sub_key = shift(@features);
        
        if (@features) {
            # Not a leaf, build up filesystem structure
            $redis->hset($store_key, $sub_key, 1);
            $store_key .= "/${sub_key}";
            next;
        }

        # Update the index
        $redis->hset($store_key, $sub_key, "${size}:${fid}");
        last;
    }

    return 1;
}



sub set {
    my ($self, $key, $value) = @_;

    $self->{error} = 0;

    my $redis = $self->{redis};
    my $furl  = $self->{furl};
    
    $key = $self->__preprocess($key);

    my $collection = $self->__get_collection($key);
    my @features = split(/\//, $key);

    my $store_key = shift(@features);

    while(1) {
        my $sub_key = shift(@features);
        
        if (@features) {
            # Not a leaf, build up filesystem structure
            $redis->hset($store_key, $sub_key, 1);
            $store_key .= "/${sub_key}";
            next;
        }
        
        # This is a leaf... a file to be stored in the warehouse
        
        # Create a new file in the collection
        my $res;

        $res = $furl->get("$self->{seaweed_addr}/dir/assign?collection=${collection}");
        return $self->__http_error($res, "failed to assign collection: $collection") unless ($res->code == &HTTP_OK);

        my $loc_info = &Cmr::JsonUtils::Decode($res->decoded_content()) or die "Failed to decode assignment";
        my $location = "http://$loc_info->{'url'}/$loc_info->{'fid'}";

        # Post the data to the file location
        my $req = $self->__hacky_multipart($location, $value);
        $res = $furl->request($req);
        return $self->__http_error($res, "failed to post file data") unless ($res->code == &HTTP_CREATED);


        my $size = length($value);

        # Update the index
        $redis->hset($store_key, $sub_key, "${size}:$loc_info->{'fid'}");
        last;
    }

    return 1;
}


sub get {
    my ($self, $key) = @_;

    $key = $self->__preprocess($key);

    my @features = split(/\//, $key);

    my $collection = $self->__get_collection($key);

    my $hash = join('/', @features[0..$#features-1]);
    my $hkey = $features[$#features];

    my $file_data = $self->{redis}->hget($hash, $hkey);
    return undef unless defined $file_data;

    my ($size,$fid) = $file_data =~ /^(\d+):(.*)$/o; # new format
    $fid //= $file_data;                             # old format
    
    my $location = $self->__fid2loc($fid, $key);
    return undef unless $location;
    my $res = $self->{furl}->get($location);
    return $self->__http_error($res, "failed to get file data") unless ($res->code ~~ [&HTTP_OK, &HTTP_PARTIAL_CONTENT]);
    
    return \$res->decoded_content;
}

sub uri_get {
    my ($self, $location) = @_;
    my $res = $self->{furl}->get($location);
    return $self->__http_error($res, "failed to get file data") unless ($res->code ~~ [&HTTP_OK, &HTTP_PARTIAL_CONTENT]);

    return \$res->decoded_content;
}

sub exists {
    my ($self, $key) = @_;
    my $location = $self->get_location($key);
    if ($location) {
        return 1;
    }
    return 0;
}

sub list {
    my ($self, $pattern) = @_;
    $pattern = $self->__preprocess($pattern);
    return $self->__traverse(pattern=>$pattern, listing=>1);
}

sub get_location {
    my ($self, $key) = @_;

    $key = $self->__preprocess($key);

    my @features = split(/\//, $key);

    my $collection = $self->__get_collection($key);

    my $hash = join('/', @features[0..$#features-1]);
    my $hkey = $features[$#features];

    my $file_data = $self->{redis}->hget($hash, $hkey);
    return undef unless defined $file_data;

    my ($size,$fid) = $file_data =~ /^(\d+):(.*)$/o; # new format
    $fid //= $file_data;                             # old format
    
    my $location = $self->__fid2loc($fid, $key);
    return $location;
}

sub get_locations {
    my ($self, $pattern) = @_;
    $pattern = $self->__preprocess($pattern);
    return $self->__traverse(pattern=>$pattern);
}

sub get_mapping {
    my ($self, $pattern) = @_;
    $pattern = $self->__preprocess($pattern);
    return $self->__traverse(pattern=>$pattern, mapping=>1);
}

sub get_pattern {
    my ($self, $pattern) = @_;
    $pattern = $self->__preprocess($pattern);

    my @locations = $self->__traverse(pattern=>$pattern);
    
    my @ret;
    for my $location (@locations) {
        my $res = $self->{furl}->get($location);
        $self->__http_error($res, "failed to get file data") unless ($res->code ~~ [&HTTP_OK, &HTTP_PARTIAL_CONTENT]);
        push @ret, \$res->decoded_content;
    }
    
    return @ret;
}

sub get_collection {
    my ($self, $key) = @_;
    $key = $self->__preprocess($key);
    return $self->__get_collection($key);
}

sub delete_hash_key {
    my ($self, $hash, $key) = @_;
    $self->{redis}->hdel("$self->{prefix}/$hash", $key);
}

sub delete_hash {
    my ($self, $hash) = @_;
    $self->{redis}->del("$self->{prefix}/$hash");
}

sub __preprocess {
    my ($self, $pattern) = @_;

    # remove leading / trailing slashes
    $pattern =~ s/^\/*//o;
    $pattern =~ s/\/$//o;

    # rebase path/pattern based on prefix
    unless ( $pattern =~ /^$self->{prefix}/ ) {
        $pattern = "$self->{prefix}/$pattern";
    }

    return $pattern;
}

sub __get_collection {
    my ($self, $key) = @_;
    my $collection;

    my @features = split(/\//, $key);
    if ($self->{prefix} eq "warehouse") {
        $collection = $features[1];
        my ($date) = $key =~ /(\d{4}-\d{2}-\d{2})/o;

        if ($date) {
            $collection = join('_', $collection, $date);
        }
    }
    elsif ($self->{prefix} eq "job") {
        $collection = "job_".$features[1];
    }
    else {
        $collection = join('_', @features[0 .. $#features-1]);
    }

    return $collection;
}

sub __fid2loc {
    my ($self, $fid, $key) = @_;

    my $collection = $self->__get_collection($key);

    unless (exists $self->{'volume_map'}{$collection}) {
        my $res = $self->{'furl'}->get("$self->{seaweed_addr}/col/lookup?collection=${collection}");
        return $self->__http_error($res, "failed to get volume map for collection: ${collection}") unless ($res->code == &HTTP_OK);
        my $collection_loc = &Cmr::JsonUtils::Decode($res->decoded_content()) or die "Failed to decode assignment";

        $self->{'volume_map'}{$collection} = $collection_loc->{$collection};
    }

    my ($vid,$id) = split(',', $fid, 2);
    my $volume_map = $self->{'volume_map'}{$collection}{$vid};

    my $idx = rand @$volume_map;
    return sprintf("http://%s:%s/%s", $volume_map->[$idx]{'Ip'}, $volume_map->[$idx]{'Port'}, $fid);
}

# Interal routines to do traversal of index
sub __traverse {
    my ($self, %args) = @_;

    my $pattern = $args{pattern};
    my $lhs = $args{lhs};
    my $rhs = $args{rhs};

    if ($pattern) {
        my @rhs = split(/\//, $pattern);
        $rhs = \@rhs;
    }

    $lhs //= [];
    
    foreach my $i (0..$#{$rhs}) {
        if ( $rhs->[$i] =~ m/$match_meta/o ) {
            next if $rhs->[$i] =~ /^\.$/o; # Special case './'
            my @new_rhs = @{$rhs}[$i+1..$#{$rhs}];
            return $self->__expand(
                pattern => $rhs->[$i],
                lhs => $lhs,
                rhs => \@new_rhs,
                mapping => $args{mapping},
                listing => $args{listing},
            );
        }
        push $lhs, $rhs->[$i];
    }

    return
};

sub __expand {
    my ($self, %args) = @_;

    my $pattern = $args{pattern};
    my $lhs = $args{lhs};
    my $rhs = $args{rhs};

    my $redis = $self->{redis};
    my $lhs_joined = join('/', @{$lhs});

    my @matched;
    if ( not $pattern or $pattern eq ".*"  ) {
        @matched = $redis->hkeys($lhs_joined);
    }
    else {
        my $reply = $redis->hkeys($lhs_joined);
        @matched = grep (/\b$pattern\b/, @$reply);
    }

    my @result;
    if (@$rhs) {
        foreach my $match (@matched) {
            if ($match =~ m/$quick_meta/o) {
                $match =~ s/$cap_meta/\\$1/g;
            }

            my @ret = $self->__traverse(
                lhs=>[@$lhs, $match],
                rhs=>$rhs,
                mapping => $args{mapping},
                listing => $args{listing},
            );

            if (@ret) {
                push @result, @ret;
            }
        }
    }
    else {
        foreach my $match (@matched) {
            my $file_data = $redis->hget($lhs_joined, $match);

            my ($size,$fid) = $file_data =~ /^(\d+):(.*)$/o; # new format
            $fid //= $file_data;                             # old format

            my $key = "$lhs_joined/$match";

            my $uri = undef;
            if ($fid ne "1") {
                $uri = $self->__fid2loc($fid, $key)
            }

            $key =~ s/$self->{prefix}\///;

            if ($args{mapping}) {
                # Only push results that map to a location
                if ($uri) {
                    push @result, [$key, $uri, $size];
                }
            }
            elsif ($args{listing}) {
                # Push results regardless of if they map to a location (results that don't map to a location are folders)
                if ($uri) {
                    push @result, [$key, $uri, $size];
                }
                else {
                    push @result, [$key, undef, undef];
                }
            }
            else {
                # Otherwise, push only the locations, no index information
                if ($uri) {
                    push @result, $uri;
                }
            }
        }
    }

    return @result;
}

sub __http_error {
    my ($self, $res, $message) = @_;
    my $warning = ($message) ? "$message [$res->{code} $res->{message}]" : "[$res->{code} $res->{message}]";
    cluck "$warning";
    $self->{errors} = 1;
    $self->{error_code}     = $res->code;
    $self->{error_message}  = $res->message;
    $self->{error_response} = $res;
    return undef;
}

__END__

=head1 NAME

Seaweed - Perl API for accessing data stored in seaweed (with redis index)

=head1 USAGE

use strict;
use warnings;

use Seaweed;

my $fs = &Seaweed::new(
    redis_addr => '10.23.30.5:7000',
    seaweed_addr => 'http://10.30.10.123:9333'
);

# Retrieve the uri for a file (picks a replica randomly)
my $location = $fs->get_location('eb_hits_sample/pdate=2015-01-01/ptime=0/test.log.gz');
print $location;

    -- or --
    
# Retrieve the uri for all files matching a regexp pattern (picks a replica randomly for each file)
my @locations = $fs->get_locations('eb_hits_sample/pdate=2015-01-01/ptime=0/.*');
print join(' ', @locations);

    -- or --

# Retrieve a mapping of files to uri's matching a regexp pattern (picks a replica randomly for each file)
my @mappings = $fs->get_mapping('eb_hits_general/pdate=2015-01-01/ptime=0/.*');
for my $mapping (@mappings) {
    my ($file, $uri) = @$mapping;
    print "${file} => ${uri}\n";
}

    -- or --

# Retrieve a file
my $data = $fs->get('eb_hits_sample/pdate=2015-01-01/ptime=0/test.log.gz');
print $$data;

    -- or --

# Retrieve all files matching a pattern
my @data = $fs->get_pattern('eb_hits_general/pdate=2015-01-01/ptime=0/.*');
for my $data (@data) {
    print $$data;
}

    -- or --
    
# Store a file
my $file = "/mnt/gv0/uhw/eb_hits_general/pdate=2015-01-01/ptime=0/test.log.gz";
my $key  = "eb_hits_general/pdate=2015-01-01/ptime=0/test.log.gz";

sysopen(IN, $file, O_RDONLY|O_BINARY ) or die "failed to open $file";
my $size = -s IN;
sysread(IN, my $data, $size);
close(IN);

$fs->set($key, $data);

    -- or --

# Store arbitrary data
my $key = "this_is_a_thing";
my $data = "that might be useful?";
$fs->set($key, $data);

