(* Utility program which copies between two block devices, using vhd BATs and efficient zero-scanning
   for performance. *)

open Xenstore

let ( +* ) = Int64.add
let ( -* ) = Int64.sub
let ( ** ) = Int64.mul
let kib = 1024L
let mib = kib ** kib

let (|>) a b = b a
module Opt = struct
	let default d = function
		| None -> d
		| Some x -> x
end
module Mutex = struct
	include Mutex
	let execute m f =
		Mutex.lock m;
		try
			let result = f () in
			Mutex.unlock m;
			result
		with e ->
			Mutex.unlock m;
			raise e
end

type logging_mode =
	| Buffer (* before we know which output format we should use *)
	| Human
	| Machine

let logging_mode = ref Buffer

let buffer = ref []

let debug_m = Mutex.create ()

let debug (fmt: ('a , unit, string, unit) format4) =
	let header = Cstruct.create Chunked.sizeof in
	Mutex.execute debug_m
		(fun () ->
			Printf.kprintf
				(fun s ->
					match !logging_mode with
						| Buffer ->
							buffer := s :: !buffer
						| Human ->
							Printf.printf "%s\n%!" s
						| Machine ->
							let data = Cstruct.create (String.length s) in
							Cstruct.blit_from_string s 0 data 0 (String.length s);
							Chunked.marshal header { Chunked.offset = 0L; data };
							Printf.printf "%s%s%!" (Cstruct.to_string header) s
				) fmt
		)

let set_logging_mode m =
	let to_flush = Mutex.execute debug_m
		(fun () ->
			logging_mode := m;
			match m with
				| Human
				| Machine ->
					List.rev !buffer
				| Buffer -> []
		) in
	List.iter (fun x -> debug "%s" x) to_flush

let startswith prefix x =
	let prefix' = String.length prefix
	and x' = String.length x in
	prefix' <= x' && (String.sub x 0 prefix' = prefix)

(** [vhd_of_device path] returns (Some vhd) where 'vhd' is the vhd leaf backing a particular device [path] or None.
    [path] may either be a blktap2 device *or* a blkfront device backed by a blktap2 device. If the latter then
    the script must be run in the same domain as blkback. *)
let vhd_of_device path =
	let find_underlying_tapdisk path =
		try 
			let open Xenstore in
		(* If we're looking at a xen frontend device, see if the backend
		   is in the same domain. If so check if it looks like a .vhd *)
			let rdev = (Unix.stat path).Unix.st_rdev in
			let major = rdev / 256 and minor = rdev mod 256 in
			let link = Unix.readlink (Printf.sprintf "/sys/dev/block/%d:%d/device" major minor) in
			match List.rev (Re_str.split (Re_str.regexp_string "/") link) with
			| id :: "xen" :: "devices" :: _ when startswith "vbd-" id ->
				let id = int_of_string (String.sub id 4 (String.length id - 4)) in
				with_xs (fun xs -> 
					let self = xs.Xs.read "domid" in
					let backend = xs.Xs.read (Printf.sprintf "device/vbd/%d/backend" id) in
					let params = xs.Xs.read (Printf.sprintf "%s/params" backend) in
					match Re_str.split (Re_str.regexp_string "/") backend with
					| "local" :: "domain" :: bedomid :: _ ->
						assert (self = bedomid);
						Some params
					| _ -> raise Not_found
				)
			| _ -> raise Not_found
		with _ -> None in
	let tapdisk_of_path path =
		try 
			match Tapctl.of_device (Tapctl.create ()) path with
			| _, _, (Some (_, vhd)) -> Some vhd
			| _, _, _ -> raise Not_found
		with Tapctl.Not_blktap ->
			debug "Device %s is not controlled by blktap" path;
			None
		| Tapctl.Not_a_device ->
			debug "%s is not a device" path;
			None
		| _ -> 
			debug "Device %s has an unknown driver" path;
			None in
	find_underlying_tapdisk path |> Opt.default path |> tapdisk_of_path

let deref_symlinks path = 
	let rec inner seen_already path = 
		if List.mem path seen_already
		then failwith "Circular symlink";
		let stats = Unix.LargeFile.lstat path in
		if stats.Unix.LargeFile.st_kind = Unix.S_LNK
		then inner (path :: seen_already) (Unix.readlink path)
		else path in
	inner [] path


(* Record when the binary started for performance measuring *)
let start = Unix.gettimeofday ()

(* Helper function to print nice progress info *)
let progress_cb =
	let last_percent = ref (-1) in

	function fraction ->
		let new_percent = int_of_float (fraction *. 100.) in
		if !last_percent <> new_percent then begin
			if !logging_mode = Machine
			then debug "Progress: %.0f" (fraction *. 100.)
			else debug "\b\rProgress: %-60s (%d%%)" (String.make (int_of_float (fraction *. 60.)) '#') new_percent;
			flush stdout;
		end;
		last_percent := new_percent

let _ =
	File.use_unbuffered := true;
	let base = ref None and src = ref None and dest = ref None and size = ref (-1L) and prezeroed = ref false in
	Arg.parse [ "-base", Arg.String (fun x -> base := Some x), "base disk to search for differences from (default: None)";
		    "-src", Arg.String (fun x -> src := Some x), "source disk";
		    "-dest", Arg.String (fun x -> dest := Some x), "destination disk";
		    "-size", Arg.String (fun x -> size := Int64.of_string x), "number of bytes to copy";
		    "-prezeroed", Arg.Set prezeroed, "assume the destination disk has been prezeroed (but not full of zeroes if [-base] is provided)";
		    "-machine", Arg.Unit (fun () -> set_logging_mode Machine), "emit machine-readable output";
	]
	(fun x -> Printf.fprintf stderr "Warning: ignoring unexpected argument %s\n" x)
	(String.concat "\n" [ "Usage:";
			      Printf.sprintf "%s [-base x] [-prezeroed] <-src y> <-dest z> <-size s>" Sys.argv.(0);
			      "  -- copy <s> bytes from <y> to <z>.";
			      "     <x> and <y> are always interpreted as filenames. If <z> is a URL then the URL";
			      "     is opened and encoded chunks of data are written directly to it";
			      "     otherwise <z> is interpreted as a filename.";
			      "";
			      "     If <-base x> is specified then only copy differences";
			      "     between <x> and <y>. If [-base x] is unspecified and [-prezeroed] is unspecified ";
			      "     then assume the destination must be fully wiped.";
			      "";
			      "Examples:";
			      "";
			      Printf.sprintf "%s -prezeroed      -src /dev/xvda -dest /dev/xvdb -size 1024" Sys.argv.(0);
			      "  -- copy 1024 bytes from /dev/xvda to /dev/xvdb assuming that /dev/xvdb is completely";
			      "     full of zeroes so there's no need to explicitly erase other parts of the disk.";
			      "";
			      Printf.sprintf "%s                 -src /dev/xvda -dest /dev/xvdb -size 1024" Sys.argv.(0);
			      "";
			      "  -- copy 1024 bytes from /dev/xvda to /dev/xvdb, always explicitly writing zeroes";
			      "     into /dev/xvdb under the assumption that it contains undefined data.";
			      "";
			      Printf.sprintf "%s -base /dev/xvdc -src /dev/xvda -dest /dev/xvdb -size 1024" Sys.argv.(0);
			      "";
			      " -- copy up to 1024 bytes of *differences* between /dev/xvdc and /dev/xvda into";
			      "     into /dev/xvdb under the assumption that /dev/xvdb contains identical data";
			      "     to /dev/xvdb."; ]);
	if !logging_mode = Buffer then set_logging_mode Human;

	let src = match !src with
		| None ->
			debug "Must have -src argument\n";
			exit 1
		| Some x -> x in
	let dest = match !dest with
		| None ->
			debug "Must have -dest argument\n";
			exit 1
		| Some x -> x in
	if !size = (-1L) then begin
		debug "Must have -size argument\n";
		exit 1
	end;
	let size = !size in
	let base = !base in

	debug "src = %s; dest = %s; base = %s; size = %Ld" src dest (Opt.default "None" base) size;
	let src_vhd = vhd_of_device src in
(* TODO: need to pause and unpause the tapdisk to make use of this
	let dest_vhd = vhd_of_device dest in
*)
	let dest_vhd = None in
	let base_vhd = match base with
		| None -> None
		| Some x -> vhd_of_device x in
	debug "src_vhd = %s; dest_vhd = %s; base_vhd = %s" (Opt.default "None" src_vhd) (Opt.default "None" dest_vhd) (Opt.default "None" base_vhd);

	let source, source_format = match src, src_vhd with
	| _, Some vhd -> vhd, "vhd"
	| device, None -> device, "raw" in
	let destination, destination_format = match dest, dest_vhd with
	| _, Some vhd -> vhd, "vhd"
	| device, None -> device, "raw" in
	let relative_to = base_vhd in

	let common = Common.make false false true in

	progress_cb 0.;
        let progress total_work work_done =
          let fraction = Int64.(to_float work_done /. (to_float total_work)) in
          progress_cb fraction in
        let t = Impl.stream_t common source relative_to source_format destination_format destination (Some "none") None !prezeroed ~progress () in
        Lwt_main.run t;
	let time = Unix.gettimeofday () -. start in
	debug "Time: %.2f seconds" time
