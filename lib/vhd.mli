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

module Feature: sig
  type t = 
    | Temporary (** A temporary disk is a candidate for deletion on shutdown *)
end

module Disk_type: sig
  type t = 
    | None
    | Reserved of int
    | Fixed_hard_disk
    | Dynamic_hard_disk
    | Differencing_hard_disk
end

module Host_OS: sig
  type t =
    | Windows
    | Macintosh
    | Other of string
end

module Geometry: sig
  type t = {
    cylinders : int;
    heads : int;
    sectors : int;
  }
end

module Footer: sig
  type t = {
    cookie : string;
    (** 8 bytes which uniquely identify the original creator of the disk image *)
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
    uid : string;
    (** 128-bit UUID *)
    saved_state : bool
    (** true if the virtual machine is in a saved (suspended) state *)
  }

end

type vhd

val create_new_dynamic: string -> int64 -> string -> ?sparse:bool -> ?table_offset:int64 ->
    ?block_size:int32 -> ?data_offset:int64 -> ?saved_state:bool -> ?features:Feature.t list ->
    unit -> vhd Lwt.t

val create_new_difference: string -> string -> string -> ?features: Feature.t list ->
    ?data_offset:int64 -> ?saved_state:bool -> ?table_offset:int64 ->
    unit -> vhd Lwt.t

val load_vhd: string -> vhd Lwt.t

val write_vhd: vhd -> unit Lwt.t

val write_sector: vhd -> int64 -> string -> unit Lwt.t

val check_overlapping_blocks: vhd -> unit

val round_up_to_2mb_block: int64 -> int64

val make_uuid: unit -> string

val really_read: Lwt_bytes.t -> int64 -> int64 -> string Lwt.t
