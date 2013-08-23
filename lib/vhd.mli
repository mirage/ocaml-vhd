(*
 * Copyright (C) 2011-2013 Citrix Inc
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)

val sector_size: int

module Feature: sig
  type t = 
    | Temporary (** A temporary disk is a candidate for deletion on shutdown *)

  val to_string : t -> string
end

module Disk_type: sig
  type t = 
    | Fixed_hard_disk
    (** A flat constant-space image *)
    | Dynamic_hard_disk
    (** An image which can grow and shrink as data is added and removed *)
    | Differencing_hard_disk
    (** An image which stores only differences from a base "parent" disk *)

  val to_string : t -> string
end

module Host_OS: sig
  type t =
    | Windows
    | Macintosh
    | Other of int32

  val to_string : t -> string
end

module Geometry: sig
  type t = {
    cylinders : int;
    heads : int;
    sectors : int;
  }

  val to_string : t -> string

  val of_sectors : int -> t
end

module Footer: sig
  type t = {
    features : Feature.t list;
    data_offset : int64;
    (** For dynamic and differencing disks, this is the absolute byte offset
        from the beginning of the file to the next structure. For fixed disks
        the value is undefined. *)
    time_stamp : int32;
    (** Creation time in seconds since midnight on January 1, 2000 *)
    creator_application : string;
    (** Name of the application which created the image *)
    creator_version : int32;
    (** Version number of the application which created the image *)
    creator_host_os : Host_OS.t;
    original_size : int64;
    (** size of the virtual disk in bytes at creation time *)
    current_size : int64;
    (** size of the virtual disk in bytes now *)
    geometry : Geometry.t;
    disk_type : Disk_type.t;
    checksum : int32;
    (** one's complement checksum of the footer with the checksum set to 0l *)
    uid : Uuidm.t;
    (** 128-bit UUID *)
    saved_state : bool
    (** true if the virtual machine is in a saved (suspended) state *)
  }

  val default_creator_application: string
  val default_creator_version: int32

  val sizeof : int
  val marshal : Cstruct.t -> t -> unit
  val unmarshal : Cstruct.t -> (t, exn) Result.t

end

module Platform_code : sig
  type t = 
    | None
    | Wi2r (** deprecated *) 
    | Wi2k (** deprecated *)
    | W2ru (** UTF-16 relative windows path *)
    | W2ku (** UTF-16 absolute windows path *)
    | Mac  (** Mac OS alias *)
    | MacX (** RFC2396 file URL *)

  val to_string : t -> string
end

module Parent_locator : sig
  type t = {
    platform_code : Platform_code.t;
    platform_data_space : int32;
    (** The number of 512-byte sectors needed to store the platform_data *)
    platform_data_space_original : int32;
    (** The original platform_data_space before automatic correction *)
    platform_data_length : int32;
    (** The length of the platform_data *)
    platform_data_offset : int64;
    (** The absolute offset of the platform_data *)
    platform_data : Cstruct.t;
  }
  val null : t
  (** No parent locator *)

  val to_string : t -> string
  val to_filename : t -> string option
  (** Attempt to read a filename from the platform_data *)

  val sizeof : int
  val marshal : Cstruct.t -> t -> unit
  val unmarshal : Cstruct.t -> t
end

module Header : sig
  type t = {
    table_offset : int64;
    (** absolute byte offset of the BAT *)
    max_table_entries : int32;
    (** the maximum number of blocks *)
    block_size : int32;
    (** size in bytes of each disk block *)
    checksum : int32;
    (** ones-complement checksum of the header *)
    parent_unique_id : Uuidm.t;
    (** if a differencing disk, this is the 128-bit UUID of the parent *)
    parent_time_stamp : int32;
    (** modification time stamp of the parent disk, as seconds since midnight Jan 1 2000 *)
    parent_unicode_name : int array;
    (** parent disk filename *)
    parent_locators : Parent_locator.t array;
    (** up to 8 different pointers to the parent disk image *)
  }
  val sizeof_bitmap : t -> int32
  val default_block_size: int32

  val sizeof : int

  val marshal : Cstruct.t -> t -> unit
  val unmarshal : Cstruct.t -> t
  val get_block_sizes : t -> int32 * int32 * int32
end

module BAT : sig
  type t = int32 array
  (** Absolute sector offset of a data block, where a data block contains
      a sector bitmap and then data *)

  val unused : int32
  (** An 'unused' BAT entry indicates no block is present *)

  val sizeof : Header.t -> int
  val unmarshal : Cstruct.t -> Header.t -> Cstruct.uint32 array
  val marshal : Cstruct.t -> t -> unit
end

module Bitmap : sig
  type t 

  val byte : t -> int -> Cstruct.t
  (** [byte t sector] is the byte containing the bit for [sector] *)

  val get : t -> int -> bool
  (** [get t sector] is true if [sector] is present in the block *)

  val set : t -> int -> unit
  (** [set t sector] asserts the bit for [sector] *)

  val clear : t -> int -> unit
  (** [clear t sector] clears the bit for [sector] *)
end

module Sector : sig
  type t = Cstruct.t
  val dump : Cstruct.t -> unit
end

module Vhd : sig
  type 'a t = {
    filename : string;
    handle : 'a;
    header : Header.t;
    footer : Footer.t;
    parent : 'a t option;
    bat : BAT.t;
  }

  val check_overlapping_blocks : 'a t -> unit
  exception EmptyVHD
  val get_top_unused_offset : Header.t -> Int32.t array -> int64
end

module Make : functor (File : S.IO) -> sig
  module Footer_IO : sig
    val read : File.fd -> int -> Footer.t File.t
    val write : File.fd -> int -> Footer.t -> unit File.t
  end
  module Parent_locator_IO : sig
    val read : File.fd -> Parent_locator.t -> Parent_locator.t File.t
    val write : File.fd -> Parent_locator.t -> unit File.t
  end
  module Header_IO : sig
    val get_parent_filename : Header.t -> string File.t
    val read : File.fd -> int -> Header.t File.t
    val write : File.fd -> 'a -> Header.t -> unit File.t
  end
  module BAT_IO : sig
    val read : File.fd -> Header.t -> Cstruct.uint32 array File.t
    val write : File.fd -> Header.t -> BAT.t -> unit File.t
  end
  module Bitmap_IO : sig
    val read : File.fd -> Header.t -> BAT.t -> int -> Cstruct.t File.t
  end
  module Vhd_IO : sig
    val openfile : string -> File.fd Vhd.t File.t
    val get_sector_pos :
      File.fd Vhd.t -> int64 -> Cstruct.t option File.t
    val write_trailing_footer : File.fd Vhd.t -> unit File.t
    val write : File.fd Vhd.t -> unit File.t
    val write_zero_block : File.fd Vhd.t -> int -> unit File.t
    val write_sector :
      File.fd Vhd.t -> int64 -> Cstruct.t -> unit File.t
  end
end
