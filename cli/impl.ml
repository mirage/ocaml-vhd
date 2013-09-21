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

(* Keep this in sync with OCaml's Unix.file_descr *)
let file_descr_of_int (x: int) : Unix.file_descr = Obj.magic x

let ( |> ) a b = b a

let parse_size x =
  let kib = 1024L in
  let mib = Int64.mul kib kib in
  let gib = Int64.mul mib kib in
  let tib = Int64.mul gib kib in
  let endswith suffix x =
    let suffix' = String.length suffix in
    let x' = String.length x in
    x' >= suffix' && (String.sub x (x' - suffix') suffix' = suffix) in
  let remove suffix x =
    let suffix' = String.length suffix in
    let x' = String.length x in
    String.sub x 0 (x' - suffix') in
  try
    if endswith "KiB" x then Int64.(mul kib (of_string (remove "KiB" x)))
    else if endswith "MiB" x then Int64.(mul mib (of_string (remove "MiB" x)))
    else if endswith "GiB" x then Int64.(mul gib (of_string (remove "GiB" x)))
    else if endswith "TiB" x then Int64.(mul tib (of_string (remove "TiB" x)))
    else Int64.of_string x
  with _ ->
    failwith (Printf.sprintf "Cannot parse size: %s" x)

module type Floatable = sig
  type t
  val to_float: t -> float
end

let hms secs =
    let h = secs / 3600 in
    let m = (secs mod 3600) / 60 in
    let s = secs mod 60 in
    Printf.sprintf "%02d:%02d:%02d" h m s

module Progress_bar(T: Floatable) = struct
  type t = {
    max_value: T.t;
    mutable current_value: T.t;
    width: int;
    line: string;
    mutable spin_index: int;
    start_time: float;
  }

  let prefix_s = "[*] "
  let prefix = String.length prefix_s
  let suffix_s = "  (   % ETA   :  :  )"
  let suffix = String.length suffix_s

  let spinner = [| '-'; '\\'; '|'; '/' |]

  let create width current_value max_value =
    let line = String.make width ' ' in
    String.blit prefix_s 0 line 0 prefix;
    String.blit suffix_s 0 line (width - suffix - 1) suffix;
    let spin_index = 0 in
    let start_time = Unix.gettimeofday () in
    { max_value; current_value; width; line; spin_index; start_time }

  let percent t = int_of_float (T.(to_float t.current_value /. (to_float t.max_value) *. 100.))

  let bar_width t value =
    int_of_float (T.(to_float value /. (to_float t.max_value) *. (float_of_int (t.width - prefix - suffix))))

  let eta t =
    let time_so_far = Unix.gettimeofday () -. t.start_time in
    let total_time = T.(to_float t.max_value /. (to_float t.current_value)) *. time_so_far in
    let remaining = int_of_float (total_time -. time_so_far) in
    hms remaining

  let print_bar t =
    let w = bar_width t t.current_value in
    t.line.[1] <- spinner.(t.spin_index);
    t.spin_index <- (t.spin_index + 1) mod (Array.length spinner);
    for i = 0 to w - 1 do
      t.line.[prefix + i] <- (if i = w - 1 then '>' else '#')
    done;
    let percent = Printf.sprintf "%3d" (percent t) in
    String.blit percent 0 t.line (t.width - 19) 3;
    let eta = eta t in
    String.blit eta 0 t.line (t.width - 10) (String.length eta);
    
    Printf.printf "\r%s%!" t.line

  let update t new_value =
    let new_value = min new_value t.max_value in
    let old_bar = bar_width t t.current_value in
    let new_bar = bar_width t new_value in
    t.current_value <- new_value;
    new_bar <> old_bar

  let average_rate t =
    let time_so_far = Unix.gettimeofday () -. t.start_time in
    T.to_float t.current_value /. time_so_far
end

open Cmdliner
open Lwt
open Vhd
open Vhd_lwt

let require name arg = match arg with
  | None -> failwith (Printf.sprintf "Please supply a %s argument" name)
  | Some x -> x

type field = {
  name: string;
  get: File.fd Vhd.t -> string Lwt.t;
}

let fields = [
  {
    name = "features";
    get = fun t -> return (String.concat ", " (List.map Feature.to_string t.Vhd.footer.Footer.features));
  }; {
    name = "data-offset";
    get = fun t -> return (Int64.to_string t.Vhd.footer.Footer.data_offset);
   }; {
    name = "time-stamp";
    get = fun t -> return (Int32.to_string t.Vhd.footer.Footer.time_stamp);
  }; {
    name = "creator-application";
    get = fun t -> return t.Vhd.footer.Footer.creator_application;
  }; {
    name = "creator-version";
    get = fun t -> return (Int32.to_string t.Vhd.footer.Footer.creator_version);
  }; {
    name = "creator-host-os";
    get = fun t -> return (Host_OS.to_string t.Vhd.footer.Footer.creator_host_os);
  }; {
    name = "original-size";
    get = fun t -> return (Int64.to_string t.Vhd.footer.Footer.original_size);
  }; {
    name = "current-size";
    get = fun t -> return (Int64.to_string t.Vhd.footer.Footer.current_size);
  }; {
    name = "geometry";
    get = fun t -> return (Geometry.to_string t.Vhd.footer.Footer.geometry);
  }; {
    name = "disk-type";
    get = fun t -> return (Disk_type.to_string t.Vhd.footer.Footer.disk_type);
  }; {
    name = "footer-checksum";
    get = fun t -> return (Int32.to_string t.Vhd.footer.Footer.checksum);
  }; {
    name = "uuid";
    get = fun t -> return (Uuidm.to_string t.Vhd.footer.Footer.uid);
  }; {
    name = "saved-state";
    get = fun t -> return (string_of_bool t.Vhd.footer.Footer.saved_state);
  }; {
    name = "table-offset";
    get = fun t -> return (Int64.to_string t.Vhd.header.Header.table_offset);
  }; {
    name = "max-table-entries";
    get = fun t -> return (string_of_int t.Vhd.header.Header.max_table_entries);
  }; {
    name = "block-size-sectors-shift";
    get = fun t -> return (string_of_int t.Vhd.header.Header.block_size_sectors_shift);
  }; {
    name = "header-checksum";
    get = fun t -> return (Int32.to_string t.Vhd.header.Header.checksum);
  }; {
    name = "parent-uuid";
    get = fun t -> return (Uuidm.to_string t.Vhd.header.Header.parent_unique_id);
  }; {
    name = "parent-time-stamp";
    get = fun t -> return (Int32.to_string t.Vhd.header.Header.parent_time_stamp);
  }; {
    name = "parent-unicode-name";
    get = fun t -> return (UTF16.to_utf8_exn t.Vhd.header.Header.parent_unicode_name);
  };
] @ (List.map (fun i ->
  {
    name = Printf.sprintf "parent-locator-%d" i;
    get = fun t -> return (Parent_locator.to_string t.Vhd.header.Header.parent_locators.(i));
  }) [ 0; 1; 2; 3; 4; 5; 6; 7 ])

let get common filename key =
  try
    let filename = require "filename" filename in
    let key = require "key" key in
    let field = List.find (fun f -> f.name = key) fields in
    let t =
      lwt t = Vhd_IO.openfile filename in
      lwt result = field.get t in
      Printf.printf "%s\n" result;
      Vhd_IO.close t in
    Lwt_main.run t;
    `Ok ()
  with
    | Failure x ->
      `Error(true, x)
    | Not_found ->
      `Error(true, Printf.sprintf "Unknown key. Known keys are: %s" (String.concat ", " (List.map (fun f -> f.name) fields)))

let padto blank n s =
  let result = String.make n blank in
  String.blit s 0 result 0 (min n (String.length s));
  result

let print_table header rows =
  let nth xs i = try List.nth xs i with Not_found -> "" in
  let width_of_column i =
    let values = nth header i :: (List.map (fun r -> nth r i) rows) in
    let widths = List.map String.length values in
    List.fold_left max 0 widths in
  let widths = List.rev (snd(List.fold_left (fun (i, acc) _ -> (i + 1, (width_of_column i) :: acc)) (0, []) header)) in
  let print_row row =
    List.iter (fun (n, s) -> Printf.printf "%s |" (padto ' ' n s)) (List.combine widths row);
    Printf.printf "\n" in
  print_row header;
  List.iter (fun (n, _) -> Printf.printf "%s-|" (padto '-' n "")) (List.combine widths header);
  Printf.printf "\n";
  List.iter print_row rows
    

let info common filename =
  try
    let filename = require "filename" filename in
    let t =
      lwt t = Vhd_IO.openfile filename in
      lwt all = Lwt_list.map_s (fun f ->
        lwt v = f.get t in
        return [ f.name; v ]
      ) fields in
      print_table ["field"; "value"] all;
      return () in
    Lwt_main.run t;
    `Ok ()
  with Failure x ->
    `Error(true, x)

let create common filename size parent =
  try
    begin let filename = require "filename" filename in
    match parent, size with
    | None, None -> failwith "Please supply either a size or a parent"
    | None, Some size ->
      let size = parse_size size in
      let t =
        lwt vhd = Vhd_IO.create_dynamic ~filename ~size () in
        Vhd_IO.close vhd in
      Lwt_main.run t
    | Some parent, None ->
      let t =
        lwt parent = Vhd_IO.openfile parent in
        lwt vhd = Vhd_IO.create_difference ~filename ~parent () in
        lwt () = Vhd_IO.close parent in
        lwt () = Vhd_IO.close vhd in
        return () in
      Lwt_main.run t
    | Some parent, Some size ->
      failwith "Overriding the size in a child node not currently implemented"
    end;
     `Ok ()
  with Failure x ->
    `Error(true, x)

let check common filename =
  try
    let filename = require "filename" filename in
    let t =
      lwt vhd = Vhd_IO.openfile filename in
      Vhd.check_overlapping_blocks vhd;
      return () in
    Lwt_main.run t;
    `Ok ()
  with Failure x ->
    `Error(true, x)

let stream_human common _ s _ _ =
  (* How much space will we need for the sector numbers? *)
  let sectors = Int64.(shift_right (add s.size.total 511L) sector_shift) in
  let decimal_digits = int_of_float (ceil (log10 (Int64.to_float sectors))) in
  Printf.printf "# stream summary:\n";
  Printf.printf "# size of the final artifact: %Ld\n" s.size.total;
  Printf.printf "# size of metadata blocks:    %Ld\n" s.size.metadata;
  Printf.printf "# size of empty space:        %Ld\n" s.size.empty;
  Printf.printf "# size of referenced blocks:  %Ld\n" s.size.copy;
  Printf.printf "# offset : contents\n";
  lwt _ = fold_left (fun sector x ->
    Printf.printf "%s: %s\n"
      (padto ' ' decimal_digits (Int64.to_string sector))
      (Element.to_string x);
    return (Int64.add sector (Element.len x))
  ) 0L s.elements in
  Printf.printf "# end of stream\n";
  return None

module P = Progress_bar(Int64)

let stream_nbd common sock s prezeroed progress =
  lwt (server, size, flags) = Nbd_lwt_client.negotiate sock in

  (* Work to do is: non-zero data to write + empty sectors if the
     target is not prezeroed *)
  let total_work = Int64.(add (add s.size.metadata s.size.copy) (if prezeroed then 0L else s.size.empty)) in
  let p = P.create 80 0L total_work in

  lwt s = if not prezeroed then expand_empty s else return s in
  lwt s = expand_copy s in

  lwt _ = fold_left (fun (sector, work_done) x ->
    lwt work = match x with
    | Element.Sectors data ->
      lwt () = Nbd_lwt_client.write server data (Int64.mul sector 512L) in
      return Int64.(of_int (Cstruct.len data))
    | Element.Empty n -> (* must be prezeroed *)
      assert prezeroed;
      return 0L
    | _ -> fail (Failure (Printf.sprintf "unexpected stream element: %s" (Element.to_string x))) in
    let sector = Int64.add sector (Element.len x) in
    let work_done = Int64.add work_done work in
    let progress_updated = P.update p work_done in
    if progress && progress_updated then P.print_bar p;
    return (sector, work_done)
  ) (0L, 0L) s.elements in
  if progress then Printf.printf "\n%!";

  lwt () = Lwt_unix.close sock in
  return (Some p)

(* Suitable for writing over the network because it doesn't lseek. Should
   merge this with Vhd_lwt.Fd.really_write *)
let really_write fd buffer =
  let ofs = buffer.Cstruct.off in
  let len = buffer.Cstruct.len in
  let buf = buffer.Cstruct.buffer in
  let rec rwrite acc fd buf ofs len =
    lwt n = Lwt_bytes.write fd buf ofs len in
    let len = len - n in
    let acc = acc + n in
    if len = 0 || n = 0
    then return acc
    else rwrite acc fd buf (ofs + n) len in
  lwt written = rwrite 0 fd buf ofs len in
  if written = 0 && len <> 0
  then fail End_of_file
  else return ()

let really_read fd buf' = 
  let ofs = buf'.Cstruct.off in
  let len = buf'.Cstruct.len in
  let buf = buf'.Cstruct.buffer in
  let rec rread fd buf ofs len = 
    lwt n = Lwt_bytes.read fd buf ofs len in
    if n = 0 then raise End_of_file;
    if n < len then rread fd buf (ofs + n) (len - n) else return () in
  lwt () = rread fd buf ofs len in
  return ()

let stream_chunked common sock s prezeroed progress =
  (* Work to do is: non-zero data to write + empty sectors if the
     target is not prezeroed *)
  let total_work = Int64.(add (add s.size.metadata s.size.copy) (if prezeroed then 0L else s.size.empty)) in
  let p = P.create 80 0L total_work in

  lwt s = if not prezeroed then expand_empty s else return s in
  lwt s = expand_copy s in

  let header = Cstruct.create Chunked.sizeof in
  lwt _ = fold_left (fun(sector, work_done) x ->
    lwt work = match x with
    | Element.Sectors data ->
      let t = { Chunked.offset = Int64.(mul sector 512L); data } in
      Chunked.marshal header t;
      lwt () = really_write sock header in
      lwt () = really_write sock data in
      return Int64.(of_int (Cstruct.len data))
    | Element.Empty n -> (* must be prezeroed *)
      assert prezeroed;
      return 0L
    | _ -> fail (Failure (Printf.sprintf "unexpected stream element: %s" (Element.to_string x))) in
    let sector = Int64.add sector (Element.len x) in
    let work_done = Int64.add work_done work in
    let progress_updated = P.update p work_done in
    if progress && progress_updated then P.print_bar p;

    return (sector, work_done)
  ) (0L, 0L) s.elements in
  if progress then Printf.printf "\n%!";

  (* Send the end-of-stream marker *)
  Chunked.marshal header { Chunked.offset = 0L; data = Cstruct.create 0 };
  lwt () = really_write sock header in

  lwt () = Lwt_unix.close sock in
  return (Some p)

let stream_raw common sock s _ progress =
  (* Work to do is: non-zero data to write + empty sectors *)
  let total_work = Int64.(add (add s.size.metadata s.size.copy) s.size.empty) in
  let p = P.create 80 0L total_work in

  lwt s = expand_empty s in
  lwt s = expand_copy s in

  lwt _ = fold_left (fun(sector, work_done) x ->
    lwt work = match x with
    | Element.Sectors data ->
      lwt () = really_write sock data in
      return Int64.(of_int (Cstruct.len data))
    | _ -> fail (Failure (Printf.sprintf "unexpected stream element: %s" (Element.to_string x))) in
    let sector = Int64.add sector (Element.len x) in
    let work_done = Int64.add work_done work in
    let progress_updated = P.update p work_done in
    if progress && progress_updated then P.print_bar p;
    return (sector, work_done)
  ) (0L, 0L) s.elements in
  if progress then Printf.printf "\n%!";

  lwt () = Lwt_unix.close sock in
  return (Some p)

type protocol = Nbd | Chunked | Human | NoProtocol
let protocol_of_string = function
  | "nbd" -> Nbd | "chunked" -> Chunked | "human" -> Human | "none" -> NoProtocol
  | x -> failwith (Printf.sprintf "Unsupported protocol: %s" x)
let string_of_protocol = function
  | Nbd -> "nbd" | Chunked -> "chunked" | Human -> "human" | NoProtocol -> "none"

type endpoint =
  | Stdout
  | File_descr of Lwt_unix.file_descr
  | Sockaddr of Lwt_unix.sockaddr
  | File of string
  | Http of Uri.t
  | Https of Uri.t

let endpoint_of_string = function
  | "stdout:" -> return Stdout
  | uri ->
    let uri' = Uri.of_string uri in
    begin match Uri.scheme uri' with
    | Some "fd" ->
      return (File_descr (Uri.path uri' |> int_of_string |> file_descr_of_int |> Lwt_unix.of_unix_file_descr))
    | Some "tcp" ->
      let host = match Uri.host uri' with None -> failwith "Please supply a host in the URI" | Some host -> host in
      let port = match Uri.port uri' with None -> failwith "Please supply a port in the URI" | Some port -> port in
      lwt host_entry = Lwt_unix.gethostbyname host in
      return (Sockaddr(Lwt_unix.ADDR_INET(host_entry.Lwt_unix.h_addr_list.(0), port)))
    | Some "unix" ->
      return (Sockaddr(Lwt_unix.ADDR_UNIX(Uri.path uri')))
    | Some "file" ->
      return (File(Uri.path uri'))
    | Some "http" ->
      return (Http uri')
    | Some "https" ->
      return (Https uri')
    | Some x ->
      fail (Failure (Printf.sprintf "Unknown URI scheme: %s" x))
    | None ->
      fail (Failure (Printf.sprintf "Failed to parse URI: %s" uri))
    end

let socket sockaddr =
  let family = match sockaddr with
  | Lwt_unix.ADDR_INET(_, _) -> Unix.PF_INET
  | Lwt_unix.ADDR_UNIX _ -> Unix.PF_UNIX
  | _ -> failwith "unsupported sockaddr type" in
  Lwt_unix.socket family Unix.SOCK_STREAM 0

let stream common (source: string) (relative_to: string option) (source_format: string) (destination_format: string) (destination: string) (source_protocol: string option) (destination_protocol: string option) prezeroed progress =
  try
    Vhd_lwt.use_odirect := common.Common.unbuffered;

    let source_protocol = require "source-protocol" source_protocol in

    let supported_formats = [ "raw"; "vhd" ] in
    let destination_protocol = match destination_protocol with
      | None -> None
      | Some x -> Some (protocol_of_string x) in
    if not (List.mem source_format supported_formats)
    then failwith (Printf.sprintf "%s is not a supported format" source_format);
    if not (List.mem destination_format supported_formats)
    then failwith (Printf.sprintf "%s is not a supported format" destination_format);

    let thread =
      lwt s = match source_format, destination_format with
        | "vhd", "vhd" ->
          lwt t = Vhd_IO.openfile source in
          lwt from = match relative_to with None -> return None | Some f -> lwt t = Vhd_IO.openfile f in return (Some t) in
          Vhd_input.vhd ?from t
        | "vhd", "raw" ->
          lwt t = Vhd_IO.openfile source in
          lwt from = match relative_to with None -> return None | Some f -> lwt t = Vhd_IO.openfile f in return (Some t) in
          Vhd_input.raw ?from t
        | "raw", "vhd" ->
          lwt t = Raw_IO.openfile source in
          Raw_input.vhd t
        | "raw", "raw" ->
          lwt t = Raw_IO.openfile source in
          Raw_input.raw t
        | _, _ -> assert false in
      lwt endpoint = endpoint_of_string destination in

      lwt (sock, possible_protocols) = match endpoint with
      | File_descr fd ->
        return (fd, [ Nbd; NoProtocol; Chunked; Human ])
      | Sockaddr sockaddr ->
        let sock = socket sockaddr in
        lwt () = Lwt_unix.connect sock sockaddr in
        return (sock, [ Nbd; NoProtocol; Chunked; Human ])
      | Http uri'
      | Https uri' ->
        (* TODO: https is not currently implemented *)
        let port = match Uri.port uri' with None -> 80 | Some port -> port in
        let host = match Uri.host uri' with None -> failwith "Please supply a host in the URI" | Some host -> host in
        lwt host_entry = Lwt_unix.gethostbyname host in
        let sockaddr = Lwt_unix.ADDR_INET(host_entry.Lwt_unix.h_addr_list.(0), port) in
        let sock = socket sockaddr in
        lwt () = Lwt_unix.connect sock sockaddr in

        let open Cohttp in
        let ic = Lwt_io.of_fd ~mode:Lwt_io.input sock in
        let oc = Lwt_io.of_fd ~mode:Lwt_io.output sock in
            
        let module Request = Request.Make(Cohttp_lwt_unix_io) in
        let module Response = Response.Make(Cohttp_lwt_unix_io) in
        let headers = Header.init () in
        let k, v = Cookie.Cookie_hdr.serialize [ "chunked", "true" ] in
        let headers = Header.add headers k v in
        let headers = match Uri.userinfo uri' with
          | None -> headers
          | Some x ->
            begin match Re_str.bounded_split_delim (Re_str.regexp_string ":") x 2 with
            | [ user; pass ] ->
              let b = Cohttp.Auth.(to_string (Basic (user, pass))) in
              Header.add headers "authorization" b
            | _ ->
              Printf.fprintf stderr "I don't know how to handle authentication for this URI.\n Try scheme://user:password@host/path\n";
              exit 1
            end in
        let request = Cohttp.Request.make ~meth:`PUT ~version:`HTTP_1_1 ~headers uri' in
        lwt () = Request.write (fun t _ -> return ()) request oc in
        begin match_lwt Response.read ic with
        | None -> fail (Failure "Unable to parse HTTP response from server")
        | Some x ->
          let code = Code.code_of_status (Cohttp.Response.status x) in
          if Code.is_success code then begin
            let advertises_nbd =
              let headers = Header.to_list (Cohttp.Response.headers x) in
              let headers = List.map (fun (x, y) -> String.lowercase x, String.lowercase y) headers in
              let te = "transfer-encoding" in
              List.mem_assoc te headers && (List.assoc te headers = "nbd") in
            if advertises_nbd
            then return(sock, [ Nbd ])
            else return(sock, [ Chunked; NoProtocol ])
          end else fail (Failure (Code.reason_phrase_of_code code))
        end in
      let destination_protocol = match destination_protocol with
        | Some x -> x
        | None ->
          let t = List.hd possible_protocols in
          Printf.fprintf stderr "Using protocol: %s\n%!" (string_of_protocol t);
          t in
      if not(List.mem destination_protocol possible_protocols)
      then fail(Failure(Printf.sprintf "this destination only supports protocols: [ %s ]" (String.concat "; " (List.map string_of_protocol possible_protocols))))
      else
        lwt p = (match destination_protocol with
            | Nbd -> stream_nbd
            | Human -> stream_human
            | Chunked -> stream_chunked
            | NoProtocol -> stream_raw) common sock s prezeroed progress in
        match p with
        | Some p ->
          if progress then begin
            let add_unit x =
              let kib = 1024. in
              let mib = kib *. 1024. in
              let gib = mib *. 1024. in
              let tib = gib *. 1024. in
              if x /. tib > 1. then Printf.sprintf "%.1f TiB" (x /. tib)
              else if x /. gib > 1. then Printf.sprintf "%.1f GiB" (x /. gib)
              else if x /. mib > 1. then Printf.sprintf "%.1f MiB" (x /. mib)
              else if x /. kib > 1. then Printf.sprintf "%.1f KiB" (x /. kib)
              else Printf.sprintf "%.1f B" x in

            Printf.printf "Time taken: %s\n" (hms (int_of_float (Unix.gettimeofday () -. p.P.start_time)));
            let physical_rate = P.average_rate p in
            Printf.printf "Physical data rate: %s/sec\n" (add_unit physical_rate);
            let speedup = Int64.(to_float s.size.total /. (to_float p.P.max_value)) in
            Printf.printf "Speedup: %.1f\n" speedup;
            Printf.printf "Virtual data rate: %s/sec\n" (add_unit (physical_rate *. speedup));
          end;
          return ()
        | None -> return () in

    Lwt_main.run thread;
    `Ok ()
  with Failure x ->
    `Error(true, x)

let serve_chunked_to_raw source dest =
  let header = Cstruct.create Chunked.sizeof in
  let twomib = 2 * 1024 * 1024 in
  let buffer = Vhd_lwt.Memory.alloc twomib in
  let rec loop () =
    lwt () = really_read source header in
    if Chunked.is_last_chunk header then begin
      Printf.fprintf stderr "Received last chunk.\n%!";
      return ()
    end else begin
      let rec block offset remaining =
        let this = Int32.(to_int (min (of_int twomib) remaining)) in
        let buf = if this < twomib then Cstruct.sub buffer 0 this else buffer in
        lwt () = really_read source buf in
        lwt () = Vhd_lwt.Fd.really_write dest offset buf in
        let offset = Int64.(add offset (of_int this)) in
        let remaining = Int32.(sub remaining (of_int this)) in
        if remaining > 0l
        then block offset remaining
        else return () in
      lwt () = block (Chunked.get_offset header) (Chunked.get_len header) in
      loop ()
    end in
  loop ()

let serve common_options source source_protocol destination destination_format =
  try
    Vhd_lwt.use_odirect := common_options.Common.unbuffered;

    let source_protocol = protocol_of_string (require "source-protocol" source_protocol) in

    let supported_formats = [ "raw" ] in
    if not (List.mem destination_format supported_formats)
    then failwith (Printf.sprintf "%s is not a supported format" destination_format);
    let supported_protocols = [ Chunked ] in
    if not (List.mem source_protocol supported_protocols)
    then failwith (Printf.sprintf "%s is not a supported source protocol" (string_of_protocol source_protocol));

    let thread =
      lwt destination_endpoint = endpoint_of_string destination in
      lwt source_endpoint = endpoint_of_string source in
      lwt source_sock = match source_endpoint with
        | File_descr fd -> return fd
        | Sockaddr s ->
          let sock = socket s in
          Lwt_unix.bind sock s;
          Lwt_unix.listen sock 1;
          lwt (fd, _) = Lwt_unix.accept sock in
          return fd
        | _ -> failwith (Printf.sprintf "Not implemented: serving from source %s" source) in
      lwt destination_fd = match destination_endpoint with
        | File path -> Vhd_lwt.Fd.openfile path
        | _ -> failwith (Printf.sprintf "Not implemented: writing to destination %s" destination) in
      serve_chunked_to_raw source_sock destination_fd in
    Lwt_main.run thread;
    `Ok ()
  with Failure x ->
  `Error(true, x)
