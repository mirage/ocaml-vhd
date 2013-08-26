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
val sector_shift: int

val max_disk_size: int64
(** Maximum size of a dynamic disk in bytes *)

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

  val of_sectors : int64 -> t
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

  val compute_checksum: t -> int32
  (** compute the expected checksum value *)

  val default_creator_application: string
  val default_creator_version: int32

  val sizeof : int
  val marshal : Cstruct.t -> t -> t
  val unmarshal : Cstruct.t -> (t, exn) Result.t

  val to_string: t -> string
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
  val unmarshal : Cstruct.t -> (t, exn) Result.t
end

module Header : sig
  type t = {
    table_offset : int64;
    (** absolute byte offset of the BAT *)
    max_table_entries : int32;
    (** the maximum number of blocks *)
    block_size_sectors_shift : int;
    (** each block is 2 ** block_size_sectors_shift sectors in size *)
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

  val to_string: t -> string

  val compute_checksum: t -> int32
  (** compute the expected checksum value *)

  val sizeof_bitmap : t -> int
  val default_block_size: int
  val default_block_size_sectors_shift: int

  val sizeof : int

  val marshal : Cstruct.t -> t -> t
  val unmarshal : Cstruct.t -> (t, exn) Result.t
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

  val get : t -> int64 -> bool
  (** [get t sector] is true if [sector] is present in the block *)

  val set : t -> int64 -> (int64 * Cstruct.t) option
  (** [set t sector] asserts the bit for [sector], returning a
      (relative offset, data to be written to disk) pair. *)

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

module Element : sig
  type 'a t =
    | Copy of ('a Vhd.t * int64 * int)
    (** copy a physical block from an underlying file *)
    | Block of Cstruct.t
    (** a new data block (e.g. for metadata) *)
    | Empty of int64
    (** empty space *)

  val to_string: 'a t -> string
end

module Make : functor (File : S.IO) -> sig
  open File
  module Footer_IO : sig
    val read : File.fd -> int64 -> Footer.t File.t
    val write : File.fd -> int64 -> Footer.t -> Footer.t File.t
  end
  module Parent_locator_IO : sig
    val read : File.fd -> Parent_locator.t -> Parent_locator.t File.t
    val write : File.fd -> Parent_locator.t -> unit File.t
  end
  module Header_IO : sig
    val get_parent_filename : Header.t -> string File.t
    val read : File.fd -> int64 -> Header.t File.t
    val write : File.fd -> int64 -> Header.t -> Header.t File.t
  end
  module BAT_IO : sig
    val read : File.fd -> Header.t -> Cstruct.uint32 array File.t
    val write : File.fd -> Header.t -> BAT.t -> unit File.t
  end
  module Bitmap_IO : sig
    val read : File.fd -> Header.t -> BAT.t -> int -> Bitmap.t File.t
  end
  module Vhd_IO : sig
    type handle

    val openfile : string -> handle Vhd.t t
    val close : handle Vhd.t -> unit t

    val create_dynamic: filename:string -> size:int64
      -> ?uuid:Uuidm.t
      -> ?saved_state:bool
      -> ?features:Feature.t list
      -> unit -> handle Vhd.t t
    (** [create_dynamic ~filename ~size] creates an empty dynamic vhd with
        virtual size [size] bytes and filename [filename]. *)

    val create_difference: filename:string -> parent:handle Vhd.t
      -> ?uuid:Uuidm.t
      -> ?saved_state:bool
      -> ?features:Feature.t list
      -> unit -> handle Vhd.t t
    (** [create_difference ~filename ~parent] creates an empty differencing vhd
        with filename [filename] backed by parent [parent]. *)

    val write : handle Vhd.t -> handle Vhd.t t
    val get_sector_location : handle Vhd.t -> int64 -> (handle Vhd.t * int64) option t
    val read_sector : handle Vhd.t -> int64 -> Cstruct.t option t
    val write_sector : handle Vhd.t -> int64 -> Cstruct.t -> unit t
  end

  type 'a stream =
    | Cons of 'a * (unit -> 'a stream t)
    | End

  val iter: ('a -> unit t) -> 'a stream -> unit t

  val raw: Vhd_IO.handle Vhd.t -> Vhd_IO.handle Element.t stream File.t

end
