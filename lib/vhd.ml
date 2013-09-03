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


(* VHD manipulation *)

let sector_size = 512
let sector_shift = 9

exception Invalid_sector of int64 * int64

module Int64 = struct
  include Int64
  let ( ++ ) = add
  let ( -- ) = sub
  let ( // ) = div
  let ( ** ) = mul
  let ( lsl ) = shift_left
  let ( lsr ) = shift_right_logical
end

let kib = 1024L
let mib = Int64.(1024L ** kib)
let gib = Int64.(1024L ** mib)
let max_disk_size = Int64.(2040L ** gib)

let kib_shift = 10
let mib_shift = 20
let gib_shift = 30

let blank_uuid = match Uuidm.of_bytes (String.make 16 '\000') with
  | Some x -> x
  | None -> assert false (* never happens *)

module Feature = struct
  type t = 
    | Temporary

  let of_int32 x =
    if Int32.logand x 1l <> 0l then [ Temporary ] else []

  let to_int32 ts =
    let one = function
      | Temporary -> 1 in
    let reserved = 2 in (* always set *)
    Int32.of_int (List.fold_left (lor) reserved (List.map one ts))

  let to_string = function
    | Temporary -> "Temporary"
end

module Disk_type = struct
  type t = 
    | Fixed_hard_disk
    | Dynamic_hard_disk
    | Differencing_hard_disk

  exception Unknown of int32

  let of_int32 =
    let open Result in function
    | 2l -> return Fixed_hard_disk 
    | 3l -> return Dynamic_hard_disk
    | 4l -> return Differencing_hard_disk
    | x -> fail (Unknown x)

  let to_int32 = function
    | Fixed_hard_disk -> 2l
    | Dynamic_hard_disk -> 3l
    | Differencing_hard_disk -> 4l

  let to_string = function
    | Fixed_hard_disk -> "Fixed_hard_disk"
    | Dynamic_hard_disk -> "Dynamic_hard_disk"
    | Differencing_hard_disk -> "Differencing_hard_disk"

end

module Host_OS = struct
  type t =
    | Windows
    | Macintosh
    | Other of int32

  let of_int32 = function
    | 0x5769326bl -> Windows
    | 0x4d616320l -> Macintosh
    | x -> Other x

  let to_int32 = function
    | Windows -> 0x5769326bl
    | Macintosh -> 0x4d616320l
    | Other x -> x

  let to_string = function
    | Windows -> "Windows"
    | Macintosh -> "Macintosh"
    | Other x -> Printf.sprintf "Other %lx" x
end

module Geometry = struct
  type t = {
    cylinders : int;
    heads : int;
    sectors : int;
  }

  (* from the Appendix 'CHS calculation' *)
  let of_sectors sectors =
    let open Int64 in
    let max_secs = 65535L ** 255L ** 16L in
    let secs = min max_secs sectors in

    let secs_per_track = ref 0L in
    let heads = ref 0L in
    let cyls_times_heads = ref 0L in
  
    if secs > 65535L ** 63L ** 16L then begin
      secs_per_track := 255L;
      heads := 16L;
      cyls_times_heads := secs // !secs_per_track;
    end else begin
      secs_per_track := 17L;
      cyls_times_heads := secs // !secs_per_track;

      heads := max ((!cyls_times_heads ++ 1023L) // 1024L) 4L;

      if (!cyls_times_heads >= (!heads ** 1024L) || !heads > 16L) then begin
        secs_per_track := 31L;
        heads := 16L;
        cyls_times_heads := secs // !secs_per_track;
      end;

      if (!cyls_times_heads >= (!heads ** 1024L)) then begin
        secs_per_track := 63L;
        heads := 16L;
        cyls_times_heads := secs // !secs_per_track;
      end	    
    end;
    { cylinders = to_int (!cyls_times_heads // !heads); heads = to_int !heads; sectors = to_int !secs_per_track }

  let to_string t = Printf.sprintf "{ cylinders = %d; heads = %d; sectors = %d }"
    t.cylinders t.heads t.sectors

end

module Checksum = struct
  type t = int32

  (* TODO: use the optimised mirage version *)
  let of_cstruct m =
    let rec inner n cur =
      if n=Cstruct.len m then cur else
        inner (n+1) (Int32.add cur (Int32.of_int (Cstruct.get_uint8 m n)))
    in 
    Int32.lognot (inner 0 0l)

  let sub_int32 t x =
    (* Adjust the checksum [t] by removing the contribution of [x] *)
    let open Int32 in
    let t' = lognot t in
    let a = logand (shift_right_logical x 0) (of_int 0xff) in
    let b = logand (shift_right_logical x 8) (of_int 0xff) in
    let c = logand (shift_right_logical x 16) (of_int 0xff) in
    let d = logand (shift_right_logical x 24) (of_int 0xff) in
    Int32.lognot (sub (sub (sub (sub t' a) b) c) d)
end

module UTF16 = struct
  type t = int array

  let to_utf8 s =
    let utf8_chars_of_int i = 
      if i < 0x80 then [char_of_int i] 
      else if i < 0x800 then 
        begin
          let z = i land 0x3f
          and y = (i lsr 6) land 0x1f in 
          [char_of_int (0xc0 + y); char_of_int (0x80+z)]
        end
      else if i < 0x10000 then
        begin
          let z = i land 0x3f
          and y = (i lsr 6) land 0x3f 
          and x = (i lsr 12) land 0x0f in
          [char_of_int (0xe0 + x); char_of_int (0x80+y); char_of_int (0x80+z)]
        end
      else if i < 0x110000 then
        begin
          let z = i land 0x3f
          and y = (i lsr 6) land 0x3f
          and x = (i lsr 12) land 0x3f
          and w = (i lsr 18) land 0x07 in
          [char_of_int (0xf0 + w); char_of_int (0x80+x); char_of_int (0x80+y); char_of_int (0x80+z)]
        end
      else
        failwith "Bad unicode character!" in
    try
      Result.Ok (String.concat "" (List.map (fun c -> Printf.sprintf "%c" c) (List.flatten (List.map utf8_chars_of_int (Array.to_list s)))))
    with e ->
      Result.Error e

  let to_string x = Printf.sprintf "[| %s |]" (String.concat "; " (List.map string_of_int (Array.to_list x)))

  let of_ascii string =
    Array.init (String.length string)
      (fun c -> int_of_char string.[c])

  let of_utf8 = of_ascii (* FIXME (obviously) *)

  let marshal (buf: Cstruct.t) t =
    let rec inner ofs n =
      if n = Array.length t
      then Cstruct.sub buf 0 ofs
      else begin
        let char = t.(n) in
        if char < 0x10000 then begin
          Cstruct.BE.set_uint16 buf ofs char;
          inner (ofs + 2) (n + 1)
        end else begin
          let char = char - 0x10000 in
          let c1 = (char lsr 10) land 0x3ff in (* high bits *)
          let c2 = char land 0x3ff in (* low bits *)
          Cstruct.BE.set_uint16 buf (ofs + 0) (0xd800 + c1);
          Cstruct.BE.set_uint16 buf (ofs + 2) (0xdc00 + c2);
          inner (ofs + 4) (n + 1)
        end
      end in
    inner 0 0

  let unmarshal (buf: Cstruct.t) len =
    (* Check if there's a byte order marker *)
    let bigendian, pos, max = match Cstruct.BE.get_uint16 buf 0 with
      | 0xfeff -> true,  2, (len / 2 - 1)
      | 0xfffe -> false, 2, (len / 2 - 1)
      | _      -> true,  0, (len / 2) in

    (* UTF-16 strings end with a \000\000 *)
    let rec strlen acc i =
      if i >= max then acc
      else
        if Cstruct.BE.get_uint16 buf i = 0
        then acc
        else strlen (acc + 1) (i + 2) in

    let max = strlen 0 0 in
    let string = Array.create max 0 in

    let rec inner ofs n =
      if n >= max then string
      else begin
        let c = Cstruct.BE.get_uint16 buf ofs in
        let code, ofs', n' =
          if c >= 0xd800 && c <= 0xdbff then begin
            let c2 = Cstruct.BE.get_uint16 buf (ofs + 2) in
            if c2 < 0xdc00 || c2 > 0xdfff then (failwith (Printf.sprintf "Bad unicode char: %04x %04x" c c2));
            let top10bits = c-0xd800 in
            let bottom10bits = c2-0xdc00 in
            let char = 0x10000 + (bottom10bits lor (top10bits lsl 10)) in
            char, ofs + 4, n + 1
          end else c, ofs + 2, n + 1 in
        string.(n) <- code;
        inner ofs' n'
      end in
    try
      Result.Ok (inner pos 0)
    with e ->
      Result.Error e
end

module Footer = struct
  type t = {
    (* "conectix" *)
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
    saved_state : bool
  }

  let to_string t = Printf.sprintf "{ features = [ %s ]; data_offset = %Lx; time_stamp = %lx; creator_application = %s; creator_version = %lx; creator_host_os = %s; original_size = %Ld; current_size = %Ld; geometry = %s; disk_type = %s; checksum = %ld; uid = %s; saved_state = %b }"
    (String.concat "; " (List.map Feature.to_string t.features)) t.data_offset t.time_stamp
    t.creator_application t.creator_version (Host_OS.to_string t.creator_host_os)
    t.original_size t.current_size (Geometry.to_string t.geometry) (Disk_type.to_string t.disk_type)
    t.checksum (Uuidm.to_string t.uid) t.saved_state

  let magic = "conectix"

  let expected_version = 0x00010000l

  let default_creator_application = "caml"
  let default_creator_version = 0x00000001l

  let dump t =
    Printf.printf "VHD FOOTER\n";
    Printf.printf "-=-=-=-=-=\n\n";
    Printf.printf "cookie              : %s\n" magic;
    Printf.printf "features            : %s\n" (String.concat "," (List.map Feature.to_string t.features));
    Printf.printf "format_version      : 0x%lx\n" expected_version;
    Printf.printf "data_offset         : 0x%Lx\n" t.data_offset;
    Printf.printf "time_stamp          : %lu\n" t.time_stamp;
    Printf.printf "creator_application : %s\n" t.creator_application;
    Printf.printf "creator_version     : 0x%lx\n" t.creator_version;
    Printf.printf "creator_host_os     : %s\n" (Host_OS.to_string t.creator_host_os);
    Printf.printf "original_size       : 0x%Lx\n" t.original_size;
    Printf.printf "current_size        : 0x%Lx\n" t.current_size;
    Printf.printf "geometry            : %s\n" (Geometry.to_string t.geometry);
    Printf.printf "disk_type           : %s\n" (Disk_type.to_string t.disk_type);
    Printf.printf "checksum            : %lu\n" t.checksum;
    Printf.printf "uid                 : %s\n" (Uuidm.to_string t.uid);
    Printf.printf "saved_state         : %b\n\n" t.saved_state

  cstruct footer {
    uint8_t magic[8];
    uint32_t features;
    uint32_t version;
    uint64_t data_offset;
    uint32_t time_stamp;
    uint8_t creator_application[4];
    uint32_t creator_version;
    uint32_t creator_host_os;
    uint64_t original_size;
    uint64_t current_size;
    uint16_t cylinders;
    uint8_t heads;
    uint8_t sectors;
    uint32_t disk_type;
    uint32_t checksum;
    uint8_t uid[16];
    uint8_t saved_state
    (* 427 zeroed *)
  } as big_endian

  let sizeof = 512

  let marshal (buf: Cstruct.t) t =
    set_footer_magic magic 0 buf;
    set_footer_features buf (Feature.to_int32 t.features);
    set_footer_version buf expected_version;
    set_footer_data_offset buf t.data_offset;
    set_footer_time_stamp buf t.time_stamp;
    set_footer_creator_application t.creator_application 0 buf;
    set_footer_creator_version buf t.creator_version;
    set_footer_creator_host_os buf (Host_OS.to_int32 t.creator_host_os);
    set_footer_original_size buf t.original_size;
    set_footer_current_size buf t.current_size;
    set_footer_cylinders buf t.geometry.Geometry.cylinders;
    set_footer_heads buf t.geometry.Geometry.heads;
    set_footer_sectors buf t.geometry.Geometry.sectors;
    set_footer_disk_type buf (Disk_type.to_int32 t.disk_type);
    set_footer_checksum buf 0l;
    set_footer_uid (Uuidm.to_bytes t.uid) 0 buf;
    set_footer_saved_state buf (if t.saved_state then 1 else 0);
    let remaining = Cstruct.shift buf sizeof_footer in
    for i = 0 to 426 do
      Cstruct.set_uint8 remaining i 0
    done;
    let checksum = Checksum.of_cstruct (Cstruct.sub buf 0 sizeof) in
    set_footer_checksum buf checksum;
    { t with checksum }

  let unmarshal (buf: Cstruct.t) =
    let open Result in
    let magic' = copy_footer_magic buf in
    ( if magic' <> magic
      then fail (Failure (Printf.sprintf "Unsupported footer cookie: expected %s, got %s" magic magic'))
      else return () ) >>= fun () ->
    let features = Feature.of_int32 (get_footer_features buf) in
    let format_version = get_footer_version buf in
    ( if format_version <> expected_version
      then fail (Failure (Printf.sprintf "Unsupported footer version: expected %lx, got %lx" expected_version format_version))
      else return () ) >>= fun () ->
    let data_offset = get_footer_data_offset buf in
    let time_stamp = get_footer_time_stamp buf in
    let creator_application = copy_footer_creator_application buf in
    let creator_version = get_footer_creator_version buf in
    let creator_host_os = Host_OS.of_int32 (get_footer_creator_host_os buf) in
    let original_size = get_footer_original_size buf in
    let current_size = get_footer_current_size buf in
    let cylinders = get_footer_cylinders buf in
    let heads = get_footer_heads buf in
    let sectors = get_footer_sectors buf in
    let geometry = { Geometry.cylinders; heads; sectors } in
    Disk_type.of_int32 (get_footer_disk_type buf) >>= fun disk_type ->
    let checksum = get_footer_checksum buf in
    let bytes = copy_footer_uid buf in
    ( match Uuidm.of_bytes bytes with
      | None -> fail (Failure (Printf.sprintf "Failed to decode UUID: %s" (String.escaped bytes)))
      | Some uid -> return uid ) >>= fun uid ->
    let saved_state = get_footer_saved_state buf = 1 in
    let expected_checksum = Checksum.(sub_int32 (of_cstruct (Cstruct.sub buf 0 sizeof)) checksum) in
    ( if checksum <> expected_checksum
      then fail (Failure (Printf.sprintf "Invalid checksum. Expected %08lx got %08lx" expected_checksum checksum))
      else return () ) >>= fun () ->
    return { features; data_offset; time_stamp; creator_version; creator_application;
      creator_host_os; original_size; current_size; geometry; disk_type; checksum; uid; saved_state }

  let compute_checksum t =
    let buf = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout sizeof)) in
    let t = marshal buf t in
    t.checksum
end

module Platform_code = struct
  type t =
    | None
    | Wi2r
    | Wi2k
    | W2ru
    | W2ku
    | Mac
    | MacX

  let wi2r = 0x57693272l
  let wi2k = 0x5769326Bl
  let w2ru = 0x57327275l
  let w2ku = 0x57326b75l
  let mac = 0x4d616320l
  let macx = 0x4d616358l

  let of_int32 =
    let open Result in function
    | 0l -> Ok None
    | x when x = wi2r -> Ok Wi2r
    | x when x = wi2k -> Ok Wi2k
    | x when x = w2ru -> Ok W2ru
    | x when x = w2ku -> Ok W2ku
    | x when x = mac -> Ok Mac
    | x when x = macx -> Ok MacX
    | x -> Error (Failure (Printf.sprintf "unknown platform_code: %lx" x))

  let to_int32 = function
    | None -> 0l
    | Wi2r -> wi2r
    | Wi2k -> wi2k
    | W2ru -> w2ru
    | W2ku -> w2ku
    | Mac -> mac
    | MacX -> macx

  let to_string = function
    | None -> "None"
    | Wi2r -> "Wi2r [deprecated]"
    | Wi2k -> "Wi2k [deprecated]"
    | W2ru -> "W2ru"
    | W2ku -> "W2ku"
    | Mac  -> "Mac "
    | MacX -> "MacX"
end

module Parent_locator = struct
  type t = {
    platform_code : Platform_code.t;

    (* WARNING WARNING - the following field is measured in *bytes* because Viridian VHDs 
       do this. This is a deviation from the spec. When reading in this field, we multiply
       by 512 if the value is less than 511 *)
    platform_data_space : int32;
    platform_data_space_original : int32; (* Original unaltered value *)

    platform_data_length : int32;
    platform_data_offset : int64;
    platform_data : Cstruct.t;
  }


  let null = {
    platform_code=Platform_code.None;
    platform_data_space=0l;
    platform_data_space_original=0l;
    platform_data_length=0l;
    platform_data_offset=0L;
    platform_data=Cstruct.create 0;
  }

  let to_string t =
    Printf.sprintf "(%s %lx %lx, %ld, 0x%Lx, %s)" (Platform_code.to_string t.platform_code)
      t.platform_data_space t.platform_data_space_original
      t.platform_data_length t.platform_data_offset (Cstruct.to_string t.platform_data)

  let to_filename t = match t.platform_code with
    | Platform_code.MacX ->
      (* Interpret as a NULL-terminated string *)
      let rec find_string from =
        if Cstruct.len t.platform_data <= from
        then t.platform_data
        else
          if Cstruct.get_uint8 t.platform_data from = 0
          then Cstruct.sub t.platform_data 0 from
          else find_string (from + 1) in
      let path = Cstruct.to_string (find_string 0) in
      let expected_prefix = "file://./" in
      let expected_prefix' = String.length expected_prefix in
      let startswith prefix x =
        let prefix' = String.length prefix and x' = String.length x in
        prefix' <= x' && (String.sub x 0 prefix' = prefix) in
      if startswith expected_prefix path
      then Some (String.sub path expected_prefix' (String.length path - expected_prefix'))
      else None
    | _ -> None

  cstruct header {
    uint32_t platform_code;
    uint32_t platform_data_space;
    uint32_t platform_data_length;
    uint32_t reserved;
    uint64_t platform_data_offset
  } as big_endian

  let sizeof = sizeof_header

  let marshal (buf: Cstruct.t) t =
    set_header_platform_code buf (Platform_code.to_int32 t.platform_code);
    set_header_platform_data_space buf (Int32.shift_right_logical t.platform_data_space sector_shift);
    set_header_platform_data_length buf t.platform_data_length;
    set_header_reserved buf 0l;
    set_header_platform_data_offset buf t.platform_data_offset

  let unmarshal (buf: Cstruct.t) =
    let open Result in
    Platform_code.of_int32 (get_header_platform_code buf) >>= fun platform_code ->
    let platform_data_space_original = get_header_platform_data_space buf in
    (* The spec says this field should be stored in sectors. However some viridian vhds
       store the value in bytes. We assume that any value we read < 512l is actually in
       sectors (511l sectors is adequate space for a filename) and any value >= 511l is
       in bytes. We store the unaltered on-disk value in [platform_data_space_original]
       and the decoded value in *bytes* in [platform_data_space]. *)
    let platform_data_space =
      if platform_data_space_original < 512l
      then Int32.shift_left platform_data_space_original sector_shift
      else platform_data_space_original in
    let platform_data_length = get_header_platform_data_length buf in
    let platform_data_offset = get_header_platform_data_offset buf in
    return { platform_code; platform_data_space_original; platform_data_space;
      platform_data_length; platform_data_offset;
      platform_data = Cstruct.create 0 }
end

module Header = struct

  type t = {
    (* cxsparse *)
    (* 0xFFFFFFFF *)
    table_offset : int64;
    (* 0x00010000l *)
    max_table_entries : int;
    block_size_sectors_shift : int;
    checksum : int32;
    parent_unique_id : Uuidm.t;
    parent_time_stamp : int32;
    parent_unicode_name : int array;
    parent_locators : Parent_locator.t array;
  }

  let to_string t =
    Printf.sprintf "{ table_offset = %Ld; max_table_entries = %d; block_size_sectors_shift = %d; checksum = %ld; parent_unique_id = %s; parent_time_stamp = %ld parent_unicode_name = %s; parent_locators = [| %s |]"
      t.table_offset t.max_table_entries t.block_size_sectors_shift t.checksum
      (Uuidm.to_string t.parent_unique_id) t.parent_time_stamp (UTF16.to_string t.parent_unicode_name)
      (String.concat "; " (List.map Parent_locator.to_string (Array.to_list t.parent_locators)))

  (* 1 bit per each 512 byte sector within the block *)
  let sizeof_bitmap t = 1 lsl (t.block_size_sectors_shift - 3)

  let magic = "cxsparse"

  let expected_data_offset = 0xFFFFFFFFFFFFFFFFL (* XXX: the spec says 8 bytes containing 0xFFFFFFFF *)

  let expected_version = 0x00010000l

  let default_block_size_sectors_shift = 12 (* 1 lsl 12 = 4096 sectors = 2 MiB *)
  let default_block_size = 1 lsl (default_block_size_sectors_shift + sector_shift)

  let dump t =
    Printf.printf "VHD HEADER\n";
    Printf.printf "-=-=-=-=-=\n";
    Printf.printf "cookie              : %s\n" magic;
    Printf.printf "data_offset         : %Lx\n" expected_data_offset;
    Printf.printf "table_offset        : %Lu\n" t.table_offset;
    Printf.printf "header_version      : 0x%lx\n" expected_version;
    Printf.printf "max_table_entries   : 0x%x\n" t.max_table_entries;
    Printf.printf "block_size          : 0x%x\n" ((1 lsl t.block_size_sectors_shift) * sector_size);
    Printf.printf "checksum            : %lu\n" t.checksum;
    Printf.printf "parent_unique_id    : %s\n" (Uuidm.to_string t.parent_unique_id);
    Printf.printf "parent_time_stamp   : %lu\n" t.parent_time_stamp;
    let s = match UTF16.to_utf8 t.parent_unicode_name with
      | Result.Ok s -> s
      | Result.Error e -> Printf.sprintf "<Unable to decode UTF-16: %s>" (String.concat " " (List.map (fun x -> Printf.sprintf "%02x" x) (Array.to_list t.parent_unicode_name))) in
    Printf.printf "parent_unicode_name : '%s' (%d bytes)\n" s (Array.length t.parent_unicode_name);
    Printf.printf "parent_locators     : %s\n" 
      (String.concat "\n                      " (List.map Parent_locator.to_string (Array.to_list t.parent_locators)))

  cstruct header {
    uint8_t magic[8];
    uint64_t data_offset;
    uint64_t table_offset;
    uint32_t header_version;
    uint32_t max_table_entries;
    uint32_t block_size;
    uint32_t checksum;
    uint8_t parent_unique_id[16];
    uint32_t parent_time_stamp;
    uint32_t reserved;
    uint8_t parent_unicode_name[512]
    (* 8 parent locators *)
    (* 256 reserved *)
  } as big_endian

  let sizeof = sizeof_header + (8 * Parent_locator.sizeof) + 256

  let unicode_offset = 8 + 8 + 8 + 4 + 4 + 4 + 4 + 16 + 4 + 4

  let marshal (buf: Cstruct.t) t =
    set_header_magic magic 0 buf;
    set_header_data_offset buf expected_data_offset;
    set_header_table_offset buf t.table_offset;
    set_header_header_version buf expected_version;
    set_header_max_table_entries buf (Int32.of_int t.max_table_entries);
    set_header_block_size buf (Int32.of_int (1 lsl (t.block_size_sectors_shift + sector_shift)));
    set_header_checksum buf 0l;
    set_header_parent_unique_id (Uuidm.to_bytes t.parent_unique_id) 0 buf;
    set_header_parent_time_stamp buf t.parent_time_stamp;
    set_header_reserved buf 0l;
    for i = 0 to 511 do
      Cstruct.set_uint8 buf (unicode_offset + i) 0
    done;
    let (_: Cstruct.t) = UTF16.marshal (Cstruct.shift buf unicode_offset) t.parent_unicode_name in
    let parent_locators = Cstruct.shift buf (unicode_offset + 512) in
    for i = 0 to 7 do
      let buf = Cstruct.shift parent_locators (Parent_locator.sizeof * i) in
      let pl = if Array.length t.parent_locators <= i then Parent_locator.null else t.parent_locators.(i) in
      Parent_locator.marshal buf pl
    done;
    let reserved = Cstruct.shift parent_locators (8 * Parent_locator.sizeof) in
    for i = 0 to 255 do
      Cstruct.set_uint8 reserved i 0
    done;
    let checksum = Checksum.of_cstruct (Cstruct.sub buf 0 sizeof) in
    set_header_checksum buf checksum;
    { t with checksum }

  let unmarshal (buf: Cstruct.t) =
    let open Result in
    let magic' = copy_header_magic buf in
    ( if magic' <> magic
      then fail (Failure (Printf.sprintf "Expected cookie %s, got %s" magic magic'))
      else return () ) >>= fun () ->
    let data_offset = get_header_data_offset buf in
    ( if data_offset <> expected_data_offset
      then fail (Failure (Printf.sprintf "Expected header data_offset %Lx, got %Lx" expected_data_offset data_offset))
      else return () ) >>= fun () ->
    let table_offset = get_header_table_offset buf in
    let header_version = get_header_header_version buf in
    ( if header_version <> expected_version
      then fail (Failure (Printf.sprintf "Expected header_version %lx, got %lx" expected_version header_version))
      else return () ) >>= fun () ->
    let max_table_entries = get_header_max_table_entries buf in
    ( if Int64.of_int32 max_table_entries > Int64.of_int Sys.max_array_length
      then fail (Failure (Printf.sprintf "expected max_table_entries < %d, got %ld" Sys.max_array_length max_table_entries))
      else return (Int32.to_int max_table_entries) ) >>= fun max_table_entries ->
    let block_size = get_header_block_size buf in
    let rec to_shift acc = function
      | 0 -> fail (Failure "block size is zero")
      | 1 -> return acc
      | n when n mod 2 = 1 -> fail (Failure (Printf.sprintf "block_size is not a power of 2: %lx" block_size))
      | n -> to_shift (acc + 1) (n / 2) in
    to_shift 0 (Int32.to_int block_size) >>= fun block_size_shift ->
    let block_size_sectors_shift = block_size_shift - sector_shift in
    let checksum = get_header_checksum buf in
    let bytes = copy_header_parent_unique_id buf in
    ( match (Uuidm.of_bytes bytes) with
      | None -> fail (Failure (Printf.sprintf "Failed to decode UUID: %s" (String.escaped bytes)))
      | Some x -> return x ) >>= fun parent_unique_id ->
    let parent_time_stamp = get_header_parent_time_stamp buf in
    UTF16.unmarshal (Cstruct.sub buf unicode_offset 512) 512 >>= fun parent_unicode_name ->
    let parent_locators_buf = Cstruct.shift buf (unicode_offset + 512) in
    let parent_locators = Array.create 8 Parent_locator.null in
    let rec loop = function
      | 8 -> return ()
      | i ->
        let buf = Cstruct.shift parent_locators_buf (Parent_locator.sizeof * i) in
        Parent_locator.unmarshal buf >>= fun p ->
        parent_locators.(i) <- p;
        loop (i + 1) in
    loop 0 >>= fun () ->
    let expected_checksum = Checksum.(sub_int32 (of_cstruct (Cstruct.sub buf 0 sizeof)) checksum) in
    ( if checksum <> expected_checksum
      then fail (Failure (Printf.sprintf "Invalid checksum. Expected %08lx got %08lx" expected_checksum checksum))
      else return () ) >>= fun () ->
    return { table_offset; max_table_entries; block_size_sectors_shift; checksum; parent_unique_id;
      parent_time_stamp; parent_unicode_name; parent_locators }

  let compute_checksum t =
    let buf = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout sizeof)) in
    let t = marshal buf t in
    t.checksum
end

module BAT = struct
  type t = {
    max_table_entries: int;
    data: Cstruct.t;
    mutable highest_value: int32;
  }

  let unused = 0xffffffffl

  let get t i = Cstruct.BE.get_uint32 t.data (i * 4)
  let set t i j =
    Cstruct.BE.set_uint32 t.data (i * 4) j;
    (* TODO: we need a proper free 'list' if we are going to allow blocks to be deallocated
       eg through TRIM *)
    if j <> unused && j > t.highest_value
    then t.highest_value <- j

  let length t = t.max_table_entries

  let equal t1 t2 =
    true
    && t1.highest_value = t2.highest_value
    && t1.max_table_entries = t2.max_table_entries
    && (try
         for i = 0 to length t1 - 1 do
           if get t1 i <> get t2 i then raise Not_found
         done;
         true
       with Not_found -> false)

  (* We always round up the size of the BAT to the next sector *)
  let sizeof_bytes (header: Header.t) =
    let size_needed = header.Header.max_table_entries * 4 in
    (* The BAT is always extended to a sector boundary *)
    ((size_needed + sector_size - 1) lsr sector_shift) lsl sector_shift

  let make (header: Header.t) =
    let data = Cstruct.create (sizeof_bytes header) in
    for i = 0 to (Cstruct.len data) / 4 - 1 do
      Cstruct.BE.set_uint32 data (i * 4) unused
    done;
    { max_table_entries = header.Header.max_table_entries; data; highest_value = -1l; }

  let to_string (t: t) =
    let used = ref [] in
    for i = 0 to length t - 1 do
      if get t i <> unused then used := (i, get t i) :: !used
    done;
    Printf.sprintf "(%d rounded to %d)[ %s ] with highest_value = %ld" (length t) (Cstruct.len t.data / 4) (String.concat "; " (List.map (fun (i, x) -> Printf.sprintf "(%d, %lx)" i x) (List.rev !used))) t.highest_value

  let unmarshal (buf: Cstruct.t) (header: Header.t) =
    let t = {
      data = buf;
      max_table_entries = header.Header.max_table_entries;
      highest_value = -1l;
    } in
    for i = 0 to length t - 1 do
      if get t i > t.highest_value then t.highest_value <- get t i
    done;
    t

  let marshal (buf: Cstruct.t) (t: t) =
    Cstruct.blit t.data 0 buf 0 (Cstruct.len t.data)
  
  let dump t =
    Printf.printf "BAT\n";
    Printf.printf "-=-\n";
    for i = 0 to t.max_table_entries - 1 do
      Printf.printf "%d\t:0x%lx\n" i (get t i)
    done
end

module Bitmap = struct
  type t =
    | Full
    | Partial of Cstruct.t

  let get t sector_in_block = match t with
    | Full -> true
    | Partial buf ->
      let sector_in_block = Int64.to_int sector_in_block in
      let bitmap_byte = Cstruct.get_uint8 buf (sector_in_block / 8) in
      let bitmap_bit = sector_in_block mod 8 in
      let mask = 0x80 lsr bitmap_bit in
      (bitmap_byte land mask) = mask

  let set t sector_in_block = match t with
    | Full -> None (* already set, no on-disk update required *)
    | Partial buf ->
      let sector_in_block = Int64.to_int sector_in_block in
      let bitmap_byte = Cstruct.get_uint8 buf (sector_in_block / 8) in
      let bitmap_bit = sector_in_block mod 8 in
      let mask = 0x80 lsr bitmap_bit in
      if (bitmap_byte land mask) = mask
      then None (* already set, no on-disk update required *)
      else begin
        (* not set, we must update the disk *)
        Cstruct.set_uint8 buf (sector_in_block / 8) (bitmap_byte lor mask);
        Some (Int64.of_int (sector_in_block / 8), Cstruct.sub buf (sector_in_block / 8) 1)
      end
end

module Sector = struct
  type t = Cstruct.t

  let dump t =
    if Cstruct.len t = 0
    then Printf.printf "Empty sector\n"
    else
      for i=0 to Cstruct.len t - 1 do
        if (i mod 16 = 15) then
          Printf.printf "%02x\n" (Cstruct.get_uint8 t i)
        else
          Printf.printf "%02x " (Cstruct.get_uint8 t i)
      done

end

module Vhd = struct
  type 'a t = {
    filename: string;
    handle: 'a;
    header: Header.t;
    footer: Footer.t;
    parent: 'a t option;
    bat: BAT.t;
  }

  let rec dump t =
    Printf.printf "VHD file: %s\n" t.filename;
    Header.dump t.header;
    Footer.dump t.footer;
    match t.parent with
    | None -> ()
    | Some parent -> dump parent

  type block_marker = 
    | Start of (string * int64)
    | End of (string * int64)

  (* Nb this only copes with dynamic or differencing disks *)
  let check_overlapping_blocks t = 
    let tomarkers name start length =
      [Start (name,start); End (name,Int64.sub (Int64.add start length) 1L)] in
    let blocks = tomarkers "footer_at_top" 0L 512L in
    let blocks = (tomarkers "header" t.footer.Footer.data_offset 1024L) @ blocks in
    let blocks =
      if t.footer.Footer.disk_type = Disk_type.Differencing_hard_disk then begin
        let locators = Array.mapi (fun i l -> (i,l)) t.header.Header.parent_locators in
        let locators = Array.to_list locators in
        let open Parent_locator in
        let locators = List.filter (fun (_,l) -> l.platform_code <> Platform_code.None) locators in
        let locations = List.map (fun (i,l) -> 
          let name = Printf.sprintf "locator block %d" i in
          let start = l.platform_data_offset in
          let length = Int64.of_int32 l.platform_data_space in
          tomarkers name start length) locators in
        (List.flatten locations) @ blocks
      end else blocks in
    let bat_start = t.header.Header.table_offset in
    let bat_size = Int64.of_int t.header.Header.max_table_entries in
    let bat = tomarkers "BAT" bat_start bat_size in
    let blocks = bat @ blocks in
    let bat_blocks = ref [] in
    for i = 0 to BAT.length t.bat - 1 do
      let e = BAT.get t.bat i in
      if e <> BAT.unused then begin
        let name = Printf.sprintf "block %d" i in
        let start = Int64.mul 512L (Int64.of_int32 (BAT.get t.bat i)) in
        let size = Int64.shift_left 1L (t.header.Header.block_size_sectors_shift + sector_shift) in
        bat_blocks := (tomarkers name start size) @ !bat_blocks
      end
    done;
    let blocks = blocks @ !bat_blocks in
    let get_pos = function | Start (_,a) -> a | End (_,a) -> a in
    let to_string = function
    | Start (name,pos) -> Printf.sprintf "%Lx START of section '%s'" pos name
    | End (name,pos) -> Printf.sprintf "%Lx END of section '%s'" pos name in
    let l = List.sort (fun a b -> compare (get_pos a) (get_pos b)) blocks in
    List.iter (fun marker -> Printf.printf "%s\n" (to_string marker)) l

  exception EmptyVHD

  let get_top_unused_offset header bat =
    let open Int64 in
    try
      let last_block_start =
        let max_entry = bat.BAT.highest_value in
        if max_entry = -1l then raise EmptyVHD;
        512L ** (of_int32 max_entry) in
      last_block_start ++ (of_int (Header.sizeof_bitmap header)) ++ (1L lsl (header.Header.block_size_sectors_shift + sector_shift))
    with 
      | EmptyVHD ->
        let pos = add header.Header.table_offset 
          (mul 4L (of_int header.Header.max_table_entries)) in
        pos

  (* TODO: need a quicker block allocator *)
  let get_free_sector header bat =
    let open Int64 in
    let next_free_byte = get_top_unused_offset header bat in
    to_int32 ((next_free_byte ++ 511L) lsr sector_shift)
end

module Element = struct
  type 'a t =
    | Copy of ('a * int64 * int)
    | Sector of Cstruct.t
    | Empty of int64

  let to_string = function
    | Copy(_, offset, 1) ->
      Printf.sprintf "1 sector copied starting at offset %Ld" offset
    | Copy(_, offset, len) ->
      Printf.sprintf "%d sectors copied starting at offset %Ld" len offset
    | Sector x ->
      Printf.sprintf "Sector \"%s...\"" (String.escaped (Cstruct.to_string (Cstruct.sub x 0 16)))
    | Empty 1L ->
      "1 empty sector"
    | Empty x ->
      Printf.sprintf "%Ld empty sectors" x

  let len = function
    | Copy(_, _, len) -> len
    | Sector _ -> 1
    | Empty x -> Int64.to_int x
end

module Make = functor(File: S.IO) -> struct
  open File

  (* Convert Result.Error values into failed threads *)
  let (>>|=) m f = match m with
    | Result.Error e -> fail e
    | Result.Ok x -> f x

  module Footer_IO = struct
    open Footer

    let read fd pos =
      really_read fd pos Footer.sizeof >>= fun buf ->
      Footer.unmarshal buf >>|= fun x ->
      return x

    let write fd pos t =
      let sector = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout Footer.sizeof)) in
      let t = Footer.marshal sector t in
      really_write fd pos sector >>= fun () ->
      return t
  end

  module Parent_locator_IO = struct
    open Parent_locator

    let read fd t =
      really_read fd t.platform_data_offset (Int32.to_int t.platform_data_length) >>= fun platform_data ->
      return { t with platform_data }

    let write fd t =
      (* Only write those that actually have a platform_code *)
      if t.platform_code <> Platform_code.None
      then really_write fd t.platform_data_offset t.platform_data
      else return ()
  end

  module Header_IO = struct
    open Header

    let get_parent_filename t =
      let rec test n =
        if n >= Array.length t.parent_locators
        then fail (Failure "Failed to find parent!")
        else
          let l = t.parent_locators.(n) in
          let open Parent_locator in
          match to_filename l with
          | Some path ->
            exists path >>= fun x ->
            if not x
            then test (n + 1)
            else return path
          | None -> test (n + 1) in
      test 0

    let read fd pos =
      really_read fd pos sizeof >>= fun buf ->
      unmarshal buf >>|= fun t -> 
      (* Read the parent_locator data *)
      let rec read_parent_locator = function
        | 8 -> return ()
        | n ->
          let p = t.parent_locators.(n) in
          let open Parent_locator in
          Parent_locator_IO.read fd p >>= fun p ->
          t.parent_locators.(n) <- p;
          read_parent_locator (n + 1) in
      read_parent_locator 0 >>= fun () ->
      return t  

    let write fd pos t =
      let buf = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout Header.sizeof)) in
      let t' = marshal buf t in
      (* Write the parent_locator data *)
      let rec write_parent_locator = function
        | 8 -> return ()
        | n ->
          let p = t.parent_locators.(n) in
          let open Parent_locator in
          Parent_locator_IO.write fd p >>= fun () ->
          write_parent_locator (n + 1) in
      really_write fd pos buf >>= fun () ->
      write_parent_locator 0 >>= fun () ->
      return t'
  end

  module BAT_IO = struct
    open BAT

    let read fd (header: Header.t) =
      really_read fd header.Header.table_offset (sizeof_bytes header) >>= fun buf ->
      return (unmarshal buf header)

    let write fd (header: Header.t) t =
      let buf = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout (sizeof_bytes header))) in
      marshal buf t;
      really_write fd header.Header.table_offset buf
  end

  module Bitmap_IO = struct
    open Bitmap

    let read fd (header: Header.t) (bat: BAT.t) (block: int) =
      let open Int64 in
      let pos = (of_int32 (BAT.get bat block)) lsl sector_shift in
      really_read fd pos (Header.sizeof_bitmap header) >>= fun bitmap ->
      return (Partial bitmap)
  end

  module Vhd_IO = struct
    open Vhd

    (* We track whether the underlying file has been closed, to avoid
       a 'use after free' style bug *)
    type handle = File.fd option ref

    exception Closed

    let get_handle t = match !(t.Vhd.handle) with
      | Some x -> return x
      | None -> fail Closed

    let write_trailing_footer handle t =
      let pos = Vhd.get_top_unused_offset t.Vhd.header t.Vhd.bat in
      Footer_IO.write handle pos t.Vhd.footer >>= fun _ ->
      return ()
    
    let write t =
      get_handle t >>= fun handle ->
      Footer_IO.write handle 0L t.Vhd.footer >>= fun footer ->
      let t ={ t with Vhd.footer } in
      Header_IO.write handle t.Vhd.footer.Footer.data_offset t.Vhd.header >>= fun header ->
      let t = { t with Vhd.header } in
      BAT_IO.write handle t.Vhd.header t.Vhd.bat >>= fun () ->
      (* Assume the data is there, or will be written later *)
      write_trailing_footer handle t >>= fun () ->
      return t

    let create_dynamic ~filename ~size
      ?(uuid = Uuidm.create `V4)
      ?(saved_state=false)
      ?(features=[]) () =

      (* The physical disk layout will be:
         byte 0   - 511:  backup footer
         byte 512 - 1535: file header
         ... empty sector-- this is where we'll put the parent locator
         byte 2048 - ...: BAT *)

      (* Use the default 2 MiB block size *)
      let block_size_sectors_shift = Header.default_block_size_sectors_shift in

      let data_offset = 512L in
      let table_offset = 2048L in

      let open Int64 in

      (* Round the size up to the nearest 2 MiB block *)
      let size = ((size ++ mib ++ mib -- 1L) lsr (1 + mib_shift)) lsl (1 + mib_shift) in
      let geometry = Geometry.of_sectors (size lsr sector_shift) in
      let creator_application = Footer.default_creator_application in
      let creator_version = Footer.default_creator_version in
      let footer = {
        Footer.features; data_offset; time_stamp = 0l; creator_application; creator_version;
        creator_host_os = Host_OS.Other 0l; original_size = size; current_size = size;
        geometry; disk_type = Disk_type.Dynamic_hard_disk;
        checksum = 0l; (* Filled in later *)
        uid = uuid; saved_state;
      } in
      let header = {
        Header.table_offset;
        max_table_entries = to_int (size lsr (block_size_sectors_shift + sector_shift));
        block_size_sectors_shift;
        checksum = 0l;
        parent_unique_id = blank_uuid;
        parent_time_stamp = 0l;
        parent_unicode_name = [| |];
        parent_locators = Array.make 8 Parent_locator.null
      } in
      let bat = BAT.make header in
      File.create filename >>= fun fd ->
      let handle = ref (Some fd) in
      let t = { filename; handle; header; footer; parent = None; bat } in
      write t >>= fun t ->
      return t

    let create_difference ~filename ~parent
      ?(uuid=Uuidm.create `V4)
      ?(saved_state=false)
      ?(features=[]) () =

      (* We use the same basic file layout as in create_dynamic *)

      let data_offset = 512L in
      let table_offset = 2048L in
      let creator_application = Footer.default_creator_application in
      let creator_version = Footer.default_creator_version in
      let footer = {
        Footer.features; data_offset;
        time_stamp = File.now ();
        creator_application; creator_version;
        creator_host_os = Host_OS.Other 0l;
        original_size = parent.Vhd.footer.Footer.current_size;
        current_size = parent.Vhd.footer.Footer.current_size;
        geometry = parent.Vhd.footer.Footer.geometry;
        disk_type = Disk_type.Differencing_hard_disk;
        checksum = 0l; uid = uuid; saved_state = saved_state; } in
      let locator0 = 
        let uri = "file://./" ^ parent.Vhd.filename in
        let platform_data = Cstruct.create (String.length uri) in
        Cstruct.blit_from_string uri 0 platform_data 0 (String.length uri);
        {
          Parent_locator.platform_code = Platform_code.MacX;
          platform_data_space = 512l;      (* bytes *)
          platform_data_space_original=1l; (* sector *)
          platform_data_length = Int32.of_int (String.length uri);
          platform_data_offset = 1536L;
          platform_data;
        } in
      File.get_modification_time parent.Vhd.filename >>= fun parent_time_stamp ->
      let header = {
        Header.table_offset;
        max_table_entries = parent.Vhd.header.Header.max_table_entries;
        block_size_sectors_shift = parent.Vhd.header.Header.block_size_sectors_shift;
        checksum = 0l;
        parent_unique_id = parent.Vhd.footer.Footer.uid;
        parent_time_stamp;
        parent_unicode_name = UTF16.of_utf8 parent.Vhd.filename;
        parent_locators = [| locator0; Parent_locator.null; Parent_locator.null; Parent_locator.null;
                             Parent_locator.null; Parent_locator.null; Parent_locator.null; Parent_locator.null; |];
      } in
      let bat = BAT.make header in
      File.create filename >>= fun fd ->
      let handle = ref (Some fd) in
      let t = { filename; handle; header; footer; parent = Some parent; bat } in
      write t >>= fun t ->
      return t

    let rec openfile filename =
      File.openfile filename >>= fun fd ->
      Footer_IO.read fd 0L >>= fun footer ->
      Header_IO.read fd (Int64.of_int Footer.sizeof) >>= fun header ->
      BAT_IO.read fd header >>= fun bat ->
      (match footer.Footer.disk_type with
        | Disk_type.Differencing_hard_disk ->
          Header_IO.get_parent_filename header >>= fun parent_filename ->
          openfile parent_filename >>= fun p ->
          return (Some p)
        | _ ->
          return None) >>= fun parent ->
      let handle = ref (Some fd) in
      return { filename; handle; header; footer; bat; parent }

    let close t =
      (* This is where we could repair the footer if we have chosen not to
         update it for speed. *)
      match !(t.Vhd.handle) with
      | Some x ->
        File.close x >>= fun () ->
        t.Vhd.handle := None;
        return ()
      | None ->
        return ()

    let rec get_sector_location t sector =
      get_handle t >>= fun handle ->
      let open Int64 in
      if sector lsl sector_shift > t.Vhd.footer.Footer.current_size
      then return None (* perhaps elements in the vhd chain have different sizes *)
      else
        let maybe_get_from_parent () = match t.Vhd.footer.Footer.disk_type,t.Vhd.parent with
          | Disk_type.Differencing_hard_disk,Some vhd2 -> get_sector_location vhd2 sector
          | Disk_type.Differencing_hard_disk,None -> fail (Failure "Sector in parent but no parent found!")
          | Disk_type.Dynamic_hard_disk,_ -> return None
          | Disk_type.Fixed_hard_disk,_ -> fail (Failure "Fixed disks are not supported") in

        let block_num = to_int (sector lsr t.Vhd.header.Header.block_size_sectors_shift) in
        let sector_in_block = rem sector (1L lsl t.Vhd.header.Header.block_size_sectors_shift) in
 
        if BAT.get t.Vhd.bat block_num = BAT.unused
        then maybe_get_from_parent ()
        else begin
          Bitmap_IO.read handle t.Vhd.header t.Vhd.bat block_num >>= fun bitmap ->
          let in_this_bitmap = Bitmap.get bitmap sector_in_block in
          match t.Vhd.footer.Footer.disk_type, in_this_bitmap with
          | _, true ->
            let data_sector = (of_int32 (BAT.get t.Vhd.bat block_num)) ++ (of_int (Header.sizeof_bitmap t.Vhd.header) lsr sector_shift) ++ sector_in_block in
            return (Some(t, data_sector lsl sector_shift))
          | Disk_type.Dynamic_hard_disk, false ->
            return None
          | Disk_type.Differencing_hard_disk, false ->
            maybe_get_from_parent ()
          | Disk_type.Fixed_hard_disk, _ -> fail (Failure "Fixed disks are not supported")
        end  

    let read_sector t sector =
      let open Int64 in
      if sector < 0L || (sector lsl sector_shift >= t.Vhd.footer.Footer.current_size)
      then fail (Invalid_sector(sector, t.Vhd.footer.Footer.current_size lsr sector_shift))
      else get_sector_location t sector >>= function
      | None -> return None
      | Some (t, offset) ->
        get_handle t >>= fun handle ->
        really_read handle offset sector_size >>= fun data ->
        return (Some data)

    let write_zero_block handle t block_num =
      let block_size_in_sectors = 1 lsl t.Vhd.header.Header.block_size_sectors_shift in
      let open Int64 in
      let bitmap_size = Header.sizeof_bitmap t.Vhd.header in
      let bitmap_sector = of_int32 (BAT.get t.Vhd.bat block_num) in

      let bitmap = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout bitmap_size)) in
      for i = 0 to bitmap_size - 1 do
        Cstruct.set_uint8 bitmap i 0
      done;
      really_write handle (bitmap_sector lsl sector_shift) bitmap >>= fun () ->

      let sector = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout 512)) in
      for i = 0 to 511 do
        Cstruct.set_uint8 sector i 0
      done;
      let rec loop n =
        if n >= block_size_in_sectors
        then return ()
        else
          let pos = (bitmap_sector lsl sector_shift) ++ (of_int bitmap_size) ++ (of_int (sector_size * n)) in
          really_write handle pos sector >>= fun () ->
          loop (n + 1) in
      loop 0

    let write_sector t sector data =
      get_handle t >>= fun handle ->
      let block_size_in_sectors = 1 lsl t.Vhd.header.Header.block_size_sectors_shift in
      let open Int64 in
      if sector < 0L || (sector lsl sector_shift >= t.Vhd.footer.Footer.current_size)
      then fail (Invalid_sector(sector, t.Vhd.footer.Footer.current_size lsr sector_shift))
      else
        let block_num = to_int (sector lsr t.Vhd.header.Header.block_size_sectors_shift) in
        assert (block_num < (BAT.length t.Vhd.bat));
        let sector_in_block = rem sector (of_int block_size_in_sectors) in
        let update_sector bitmap_sector =
          let bitmap_sector = of_int32 bitmap_sector in
          let data_sector = bitmap_sector ++ (of_int (Header.sizeof_bitmap t.Vhd.header) lsr sector_shift) ++ sector_in_block in
          Bitmap_IO.read handle t.Vhd.header t.Vhd.bat block_num >>= fun bitmap ->
          really_write handle (data_sector lsl sector_shift) data >>= fun () ->
          match Bitmap.set bitmap sector_in_block with
          | None -> return ()
          | Some (offset, buf) -> really_write handle ((bitmap_sector lsl sector_shift) ++ offset) buf in

        if BAT.get t.Vhd.bat block_num = BAT.unused then begin
          BAT.set t.Vhd.bat block_num (Vhd.get_free_sector t.Vhd.header t.Vhd.bat);
          write_zero_block handle t block_num >>= fun () ->
          BAT_IO.write handle t.Vhd.header t.Vhd.bat >>= fun () ->
          write_trailing_footer handle t >>= fun () ->
          update_sector (BAT.get t.Vhd.bat block_num)
        end else begin
          update_sector (BAT.get t.Vhd.bat block_num)
        end
  end

  type 'a stream =
    | Cons of 'a * (unit -> 'a stream t)
    | End

  let rec iter f = function
    | Cons(x, rest) ->
      f x >>= fun () ->
      rest () >>= fun x ->
      iter f x
    | End ->
      return ()

  let rec fold_left f initial xs = match xs with
    | End -> return initial
    | Cons (x, rest) ->
      f initial x >>= fun initial' ->
      rest () >>= fun xs ->
      fold_left f initial' xs

  open Element

  (* Test whether a block is in any BAT in the path to the root. If so then we will
     look up all sectors. *)
  let rec in_any_bat vhd i = match BAT.get vhd.Vhd.bat i <> BAT.unused, vhd.Vhd.parent with
    | true, _ -> true
    | false, Some parent -> in_any_bat parent i
    | false, None -> false

  let rec coalesce_request acc s =
    s >>= fun next -> match next, acc with
    | End, None -> return End
    | End, Some x -> return (Cons(x, fun () -> return End))
    | Cons(Sector s, next), None -> return(Cons(Sector s, fun () -> coalesce_request None (next ())))
    | Cons(Sector _, next), Some x -> return(Cons(x, fun () -> coalesce_request None s))
    | Cons(Empty n, next), None -> coalesce_request (Some(Empty n)) (next ())
    | Cons(Empty n, next), Some(Empty m) -> coalesce_request (Some(Empty (Int64.add n m))) (next ())
    | Cons(Empty n, next), Some x -> return (Cons(x, fun () -> coalesce_request None s))
    | Cons(Copy(h, ofs, len), next), None -> coalesce_request (Some (Copy(h, ofs, len))) (next ())
    | Cons(Copy(h, ofs, len), next), Some(Copy(h', ofs', len')) ->
      if Int64.(add ofs (of_int len)) = ofs' && h == h'
      then coalesce_request (Some(Copy(h, ofs, len + len'))) (next ())
      else if Int64.(add ofs' (of_int len')) = ofs && h == h'
      then coalesce_request (Some(Copy(h, ofs', len + len'))) (next ())
      else return (Cons(Copy(h', ofs', len'), fun () -> coalesce_request None s))
    | Cons(Copy(h, ofs, len), next), Some x -> return(Cons(x, fun () -> coalesce_request None s))

  let raw (vhd: Vhd_IO.handle Vhd.t) =
    let block_size_sectors_shift = vhd.Vhd.header.Header.block_size_sectors_shift in
    let max_table_entries = vhd.Vhd.header.Header.max_table_entries in
    let empty_block = Empty (Int64.shift_left 1L block_size_sectors_shift) in
    let empty_sector = Empty 1L in

    let rec block i =
      let next_block () = block (i + 1) in
      if i = max_table_entries
      then return End
      else begin
        if not(in_any_bat vhd i)
        then return (Cons(empty_block, next_block))
        else begin
          let rec sector j =
            let next_sector () = sector (j + 1) in
            if j = 1 lsl block_size_sectors_shift
            then next_block ()
            else begin
              let absolute_sector = Int64.(add (shift_left (of_int i) block_size_sectors_shift) (of_int j)) in
              Vhd_IO.get_sector_location vhd absolute_sector >>= function
              | None ->
                return (Cons(empty_sector, next_sector))
              | Some (vhd', offset) ->
                Vhd_IO.get_handle vhd' >>= fun handle ->
                return (Cons(Copy(handle, Int64.shift_right offset sector_shift, 1), next_sector))
            end in
          sector 0
        end
      end in
    coalesce_request None (block 0)

  let vhd (t: Vhd_IO.handle Vhd.t) =
    let block_size_sectors_shift = t.Vhd.header.Header.block_size_sectors_shift in
    let max_table_entries = t.Vhd.header.Header.max_table_entries in

    (* The physical disk layout will be:
       byte 0   - 511:  backup footer
       byte 512 - 1535: file header
       ... empty sector-- this is where we'll put the parent locator
       byte 2048 - ...: BAT *)

    let data_offset = 512L in
    let table_offset = 2048L in

    let size = t.Vhd.footer.Footer.current_size in
    let geometry = Geometry.of_sectors (Int64.shift_right_logical size sector_shift) in
    let creator_application = Footer.default_creator_application in
    let creator_version = Footer.default_creator_version in
    let uuid = Uuidm.create `V4 in

    let footer = {
      Footer.features = [];
      data_offset;
      time_stamp = 0l; creator_application; creator_version;
      creator_host_os = Host_OS.Other 0l;
      original_size = size; current_size = size;
      geometry; disk_type = Disk_type.Dynamic_hard_disk;
      checksum = 0l; (* Filled in later *)
      uid = uuid; saved_state = false;
    } in

    let header = {
      Header.table_offset; max_table_entries; block_size_sectors_shift;
      checksum = 0l;
      parent_unique_id = blank_uuid;
      parent_time_stamp = 0l;
      parent_unicode_name = [| |];
      parent_locators = [| Parent_locator.null; Parent_locator.null; Parent_locator.null; Parent_locator.null;
                           Parent_locator.null; Parent_locator.null; Parent_locator.null; Parent_locator.null; |];
    } in
    let bat = BAT.make header in

    let sizeof_bat = BAT.sizeof_bytes header in

    let sizeof_bitmap = Header.sizeof_bitmap header in
    (* We'll always set all bitmap bits *)
    let bitmap = Cstruct.create sizeof_bitmap in
    for i = 0 to sizeof_bitmap - 1 do
      Cstruct.set_uint8 bitmap i 0xff
    done;
    let sizeof_data_sectors = 1 lsl block_size_sectors_shift in
    let sizeof_data = 1 lsl (block_size_sectors_shift + sector_shift) in

    (* Calculate where the first data block will go. Note the sizeof_bat is already
       rounded up to the next sector boundary. *)
    let first_block = Int64.(table_offset ++ (of_int sizeof_bat)) in
    let next_byte = ref first_block in
    for i = 0 to max_table_entries - 1 do
      if in_any_bat t i then begin
        BAT.set bat i (Int64.(to_int32(!next_byte lsr sector_shift)));
        next_byte := Int64.(!next_byte ++ (of_int sizeof_bitmap) ++ (of_int sizeof_data))
      end
    done;

    let rec write_sectors buf from andthen =
      if from >= (Cstruct.len buf)
      then andthen ()
      else
        let sector = Cstruct.sub buf from 512 in
        return(Cons(Sector sector, fun () -> write_sectors buf (from + 512) andthen)) in

    let rec block i andthen =
      let rec sector j =
        let next () = if j = sizeof_data_sectors - 1 then block (i + 1) andthen else sector (j + 1) in
        let absolute_sector = Int64.(add (shift_left (of_int i) block_size_sectors_shift) (of_int j)) in
        Vhd_IO.get_sector_location t absolute_sector >>= function
        | None ->
          return (Cons(Empty 1L, next))
        | Some (vhd', offset) ->
          Vhd_IO.get_handle vhd' >>= fun handle ->
          return (Cons(Copy(handle, Int64.shift_right offset sector_shift, 1), next)) in
      if i >= header.Header.max_table_entries
      then andthen ()
      else
        if BAT.get bat i <> BAT.unused
        then return(Cons(Sector bitmap, fun () -> sector 0))
        else block (i + 1) andthen in

    assert(Footer.sizeof = 512);
    assert(Header.sizeof = 1024);

    let buf = Cstruct.create (max Footer.sizeof (max Header.sizeof sizeof_bat)) in
    let (_: Footer.t) = Footer.marshal buf footer in
    coalesce_request None (return (Cons(Sector(Cstruct.sub buf 0 Footer.sizeof), fun () ->
      let (_: Header.t) = Header.marshal buf header in
      write_sectors (Cstruct.sub buf 0 Header.sizeof) 0 (fun () ->
        return(Cons(Empty 1L, fun () ->
          BAT.marshal buf bat;
          write_sectors (Cstruct.sub buf 0 sizeof_bat) 0 (fun () ->
            let (_: Footer.t) = Footer.marshal buf footer in
            block 0 (fun () ->
              return(Cons(Sector(Cstruct.sub buf 0 Footer.sizeof), fun () -> return End))
            )
          )
       ))
     )
   )))
end
