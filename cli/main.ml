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

let project_url = "http://github.com/djs55/ocaml-vhd"

open Common
open Cmdliner

(* Help sections common to all commands *)

let _common_options = "COMMON OPTIONS"
let help = [ 
 `S _common_options; 
 `P "These options are common to all commands.";
 `S "MORE HELP";
 `P "Use `$(mname) $(i,COMMAND) --help' for help on a single command."; `Noblank;
 `S "BUGS"; `P (Printf.sprintf "Check bug reports at %s" project_url);
]

(* Options common to all commands *)
let common_options_t = 
  let docs = _common_options in 
  let debug = 
    let doc = "Give only debug output." in
    Arg.(value & flag & info ["debug"] ~docs ~doc) in
  let verb =
    let doc = "Give verbose output." in
    let verbose = true, Arg.info ["v"; "verbose"] ~docs ~doc in 
    Arg.(last & vflag_all [false] [verbose]) in 
  Term.(pure Common.make $ debug $ verb)

module Impl = struct
  open Lwt
  open Vhd
  open Vhd_lwt

let info common filename =
  `Ok ()

  let require name arg = match arg with
    | None -> failwith (Printf.sprintf "Please supply a %s argument" name)
    | Some x -> x

  let create common filename size parent =
    try
      begin let filename = require "filename" filename in
      match parent, size with
      | None, None -> failwith "Please supply either a size or a parent"
      | None, Some size ->
        let size = Int64.of_string size in
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

let stream common filename format =
  failwith "unimplemented"
end

let info_cmd =
  let doc = "display general information about a vhd" in
  let man = [
    `S "DESCRIPTION";
    `P "Display general information about a vhd, including header and footer fields. This won't directly display block allocation tables or sector bitmaps.";
  ] @ help in
  let filename =
    let doc = Printf.sprintf "Path to the vhd file." in
    Arg.(value & pos 0 (some file) None & info [] ~doc) in
  Term.(ret(pure Impl.info $ common_options_t $ filename)),
  Term.info "info" ~sdocs:_common_options ~doc ~man

let create_cmd =
  let doc = "create a dynamic vhd" in
  let man = [
    `S "DESCRIPTION";
    `P "Create a dynamic vhd (i.e. one which may be sparse). A dynamic vhd may be self-contained or it may have a backing-file or 'parent'.";
  ] @ help in
  let filename =
    let doc = Printf.sprintf "Path to the vhd file to be created." in
    Arg.(value & pos 0 (some string) None & info [] ~doc) in
  let size =
    let doc = Printf.sprintf "Virtual size of the disk." in
    Arg.(value & opt (some string) None & info [ "size" ] ~doc) in
  let parent =
    let doc = Printf.sprintf "Parent image" in
    Arg.(value & opt (some file) None & info [ "parent" ] ~doc) in
  Term.(ret(pure Impl.create $ common_options_t $ filename $ size $ parent)),
  Term.info "create" ~sdocs:_common_options ~doc ~man

let check_cmd =
  let doc = "check the structure of a vhd file" in
  let man = [
    `S "DESCRIPTION";
    `P "Check the structure of a vhd file is valid, print any errors on the console.";
  ] @ help in
  let filename =
    let doc = Printf.sprintf "Path to the vhd to be checked." in
    Arg.(value & pos 0 (some file) None & info [] ~doc) in
  Term.(ret(pure Impl.check $ common_options_t $ filename)),
  Term.info "check" ~sdocs:_common_options ~doc ~man

let stream_cmd =
  let doc = "stream the contents of a vhd disk" in
  let man = [
    `S "DESCRIPTION";
    `P "Stream the contents of a virtual disk defined by the given vhd filename and all of its parents. The default streaming format is \"english\" where the contents are described as a sequence of I/O operations such as \"insert sector 5 from file x.vhd\". Other streaming formats are: \"raw\" and \"vhd\".";
  ] @ help in
  let format =
    let doc = "Output format" in
    Arg.(value & opt (some string) (Some "english") & info [ "format" ] ~doc) in
  let filename =
    let doc = Printf.sprintf "Path to the vhd to be streamed." in
    Arg.(value & pos 0 (some file) None & info [] ~doc) in
  Term.(ret(pure Impl.stream $ common_options_t $ filename $ format)),
  Term.info "stream" ~sdocs:_common_options ~doc ~man


let default_cmd = 
  let doc = "manipulate virtual disks stored in vhd files" in 
  let man = help in
  Term.(ret (pure (fun _ -> `Help (`Pager, None)) $ common_options_t)),
  Term.info "vhd-tool" ~version:"1.0.0" ~sdocs:_common_options ~doc ~man
       
let cmds = [info_cmd; create_cmd; check_cmd; stream_cmd]

let _ =
  match Term.eval_choice default_cmd cmds with 
  | `Error _ -> exit 1
  | _ -> exit 0
