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
(*
module type Feature =
  sig
    type t = Temporary
    val of_int32 : int32 -> t list
    val to_int32 : t list -> int32
    val to_string : t -> string
  end
module type Disk_type =
  sig
    type t =
        None
      | Reserved of int
      | Fixed_hard_disk
      | Dynamic_hard_disk
      | Differencing_hard_disk
    val of_int32 : int32 -> t
    val to_int32 : t -> int32
    val to_string : t -> string
  end
module type Host_OS =
  sig
    type t = Windows | Macintosh | Other of int32
    val of_int32 : int32 -> t
    val to_int32 : t -> int32
    val to_string : t -> string
  end
module type Geometry =
  sig
    type t = { cylinders : int; heads : int; sectors : int; }
    val of_sectors : int -> t
    val to_string : t -> string
  end
module type UTF16 =
  sig
    type t = int array
    val to_utf8 : int array -> string
    val marshal : Cstruct.t -> Cstruct.uint16 array -> Cstruct.t
    val unmarshal : Cstruct.t -> int -> Cstruct.uint16 array
  end
module type Footer =
  sig
    type t = {
      features : Feature.t list;
      data_offset : int64;
      time_stamp : int32;
      creator_application : string;
      creator_version : int32;
      creator_host_os : Host_OS.t;
      original_size : int64;
      current_size : int64;
      geometry : Geometry.t;
      disk_type : Disk_type.t;
      checksum : int32;
      uid : Uuidm.t;
      saved_state : bool;
    }
    val magic : string
    val expected_version : int32
    val dump : t -> unit
    val get_footer_magic : Cstruct.t -> Cstruct.t
    val copy_footer_magic : Cstruct.t -> string
    val set_footer_magic : string -> int -> Cstruct.t -> unit
    val blit_footer_magic : Cstruct.t -> int -> Cstruct.t -> unit
    val get_footer_features : Cstruct.t -> Cstruct.uint32
    val set_footer_features : Cstruct.t -> Cstruct.uint32 -> unit
    val get_footer_version : Cstruct.t -> Cstruct.uint32
    val set_footer_version : Cstruct.t -> Cstruct.uint32 -> unit
    val get_footer_data_offset : Cstruct.t -> Cstruct.uint64
    val set_footer_data_offset : Cstruct.t -> Cstruct.uint64 -> unit
    val get_footer_time_stamp : Cstruct.t -> Cstruct.uint32
    val set_footer_time_stamp : Cstruct.t -> Cstruct.uint32 -> unit
    val get_footer_creator_application : Cstruct.t -> Cstruct.t
    val copy_footer_creator_application : Cstruct.t -> string
    val set_footer_creator_application : string -> int -> Cstruct.t -> unit
    val blit_footer_creator_application :
      Cstruct.t -> int -> Cstruct.t -> unit
    val get_footer_creator_version : Cstruct.t -> Cstruct.uint32
    val set_footer_creator_version : Cstruct.t -> Cstruct.uint32 -> unit
    val get_footer_creator_host_os : Cstruct.t -> Cstruct.uint32
    val set_footer_creator_host_os : Cstruct.t -> Cstruct.uint32 -> unit
    val get_footer_original_size : Cstruct.t -> Cstruct.uint64
    val set_footer_original_size : Cstruct.t -> Cstruct.uint64 -> unit
    val get_footer_current_size : Cstruct.t -> Cstruct.uint64
    val set_footer_current_size : Cstruct.t -> Cstruct.uint64 -> unit
    val get_footer_cylinders : Cstruct.t -> Cstruct.uint16
    val set_footer_cylinders : Cstruct.t -> Cstruct.uint16 -> unit
    val get_footer_heads : Cstruct.t -> Cstruct.uint8
    val set_footer_heads : Cstruct.t -> Cstruct.uint8 -> unit
    val get_footer_sectors : Cstruct.t -> Cstruct.uint8
    val set_footer_sectors : Cstruct.t -> Cstruct.uint8 -> unit
    val get_footer_disk_type : Cstruct.t -> Cstruct.uint32
    val set_footer_disk_type : Cstruct.t -> Cstruct.uint32 -> unit
    val get_footer_checksum : Cstruct.t -> Cstruct.uint32
    val set_footer_checksum : Cstruct.t -> Cstruct.uint32 -> unit
    val get_footer_uid : Cstruct.t -> Cstruct.t
    val copy_footer_uid : Cstruct.t -> string
    val set_footer_uid : string -> int -> Cstruct.t -> unit
    val blit_footer_uid : Cstruct.t -> int -> Cstruct.t -> unit
    val sizeof_footer : int
    val get_footer_saved_state : Cstruct.t -> Cstruct.uint8
    val set_footer_saved_state : Cstruct.t -> Cstruct.uint8 -> unit
    val sizeof : int
    val marshal : Cstruct.t -> t -> unit
    val unmarshal : Cstruct.t -> t
  end
module type Platform_code =
  sig
    type t = None | Wi2r | Wi2k | W2ru | W2ku | Mac | MacX
    val wi2r : int32
    val wi2k : int32
    val w2ru : int32
    val w2ku : int32
    val mac : int32
    val macx : int32
    val of_int32 : int32 -> t
    val to_int32 : t -> int32
    val to_string : t -> string
  end
module type Parent_locator =
  sig
    type t = {
      platform_code : Platform_code.t;
      platform_data_space : int32;
      platform_data_space_original : int32;
      platform_data_length : int32;
      platform_data_offset : int64;
      platform_data : Cstruct.t;
    }
    val null : t
    val to_string : t -> string
    val to_filename : t -> string option
    val get_header_platform_code : Cstruct.t -> Cstruct.uint32
    val set_header_platform_code : Cstruct.t -> Cstruct.uint32 -> unit
    val get_header_platform_data_space : Cstruct.t -> Cstruct.uint32
    val set_header_platform_data_space : Cstruct.t -> Cstruct.uint32 -> unit
    val get_header_platform_data_length : Cstruct.t -> Cstruct.uint32
    val set_header_platform_data_length : Cstruct.t -> Cstruct.uint32 -> unit
    val get_header_reserved : Cstruct.t -> Cstruct.uint32
    val set_header_reserved : Cstruct.t -> Cstruct.uint32 -> unit
    val sizeof_header : int
    val get_header_platform_data_offset : Cstruct.t -> Cstruct.uint64
    val set_header_platform_data_offset : Cstruct.t -> Cstruct.uint64 -> unit
    val sizeof : int
    val marshal : Cstruct.t -> t -> unit
    val unmarshal : Cstruct.t -> t
  end
module type Header =
  sig
    type t = {
      table_offset : int64;
      max_table_entries : int32;
      block_size : int32;
      checksum : int32;
      parent_unique_id : Uuidm.t;
      parent_time_stamp : int32;
      parent_unicode_name : int array;
      parent_locators : Parent_locator.t array;
    }
    val sizeof_bitmap : t -> int32
    val magic : string
    val expected_data_offset : int64
    val expected_version : int32
    val dump : t -> unit
    val get_header_magic : Cstruct.t -> Cstruct.t
    val copy_header_magic : Cstruct.t -> string
    val set_header_magic : string -> int -> Cstruct.t -> unit
    val blit_header_magic : Cstruct.t -> int -> Cstruct.t -> unit
    val get_header_data_offset : Cstruct.t -> Cstruct.uint64
    val set_header_data_offset : Cstruct.t -> Cstruct.uint64 -> unit
    val get_header_table_offset : Cstruct.t -> Cstruct.uint64
    val set_header_table_offset : Cstruct.t -> Cstruct.uint64 -> unit
    val get_header_header_version : Cstruct.t -> Cstruct.uint32
    val set_header_header_version : Cstruct.t -> Cstruct.uint32 -> unit
    val get_header_max_table_entries : Cstruct.t -> Cstruct.uint32
    val set_header_max_table_entries : Cstruct.t -> Cstruct.uint32 -> unit
    val get_header_block_size : Cstruct.t -> Cstruct.uint32
    val set_header_block_size : Cstruct.t -> Cstruct.uint32 -> unit
    val get_header_checksum : Cstruct.t -> Cstruct.uint32
    val set_header_checksum : Cstruct.t -> Cstruct.uint32 -> unit
    val get_header_parent_unique_id : Cstruct.t -> Cstruct.t
    val copy_header_parent_unique_id : Cstruct.t -> string
    val set_header_parent_unique_id : string -> int -> Cstruct.t -> unit
    val blit_header_parent_unique_id : Cstruct.t -> int -> Cstruct.t -> unit
    val get_header_parent_time_stamp : Cstruct.t -> Cstruct.uint32
    val set_header_parent_time_stamp : Cstruct.t -> Cstruct.uint32 -> unit
    val get_header_reserved : Cstruct.t -> Cstruct.uint32
    val set_header_reserved : Cstruct.t -> Cstruct.uint32 -> unit
    val sizeof_header : int
    val get_header_parent_unicode_name : Cstruct.t -> Cstruct.t
    val copy_header_parent_unicode_name : Cstruct.t -> string
    val set_header_parent_unicode_name : string -> int -> Cstruct.t -> unit
    val blit_header_parent_unicode_name :
      Cstruct.t -> int -> Cstruct.t -> unit
    val sizeof : int
    val unicode_offset : int
    val marshal : Cstruct.t -> t -> unit
    val unmarshal : Cstruct.t -> t
    val get_block_sizes : t -> int32 * int32 * int32
  end
module type BAT =
  sig
    type t = int32 array
    val unused : int32
    val sizeof : Header.t -> int
    val unmarshal : Cstruct.t -> Header.t -> Cstruct.uint32 array
    val marshal : Cstruct.t -> t -> unit
    val dump : int32 array -> unit
  end
module type Bitmap =
  sig
    type t = Cstruct.t
    val sector : Cstruct.t -> int -> Cstruct.t
    val get : Cstruct.t -> int -> bool
    val set : Cstruct.t -> int -> unit
    val clear : Cstruct.t -> int -> unit
  end
module type Sector = sig type t = Cstruct.t val dump : Cstruct.t -> unit end
module type Vhd =
  sig
    type 'a t = {
      filename : string;
      handle : 'a;
      header : Header.t;
      footer : Footer.t;
      parent : 'a t option;
      bat : BAT.t;
    }
    val get_offset_info_of_sector :
      'a t -> int64 -> int * int * int * int * int * int * int64 * int64
    val dump : 'a t -> unit
    type block_marker = Start of (string * int64) | End of (string * int64)
    val check_overlapping_blocks : 'a t -> unit
    exception EmptyVHD
    val get_top_unused_offset : Header.t -> Int32.t array -> int64
  end
*)

module type ASYNC = sig
  type 'a t

  val (>>=): 'a t -> ('a -> 'b t) -> 'b t
  val fail: exn -> 'a t
  val return: 'a -> 'a t
end

module type IO = sig
  include ASYNC

  type fd

  val exists: string -> bool t
  val openfile: string -> fd t
  val really_read: fd -> int64 -> int -> Cstruct.t t
  val really_write: fd -> int64 -> Cstruct.t -> unit t
end

