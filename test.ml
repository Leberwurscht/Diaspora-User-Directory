(* get the command line options *)
let databasedir = Sys.argv.(1);;
let logfile = Sys.argv.(2);;

(* set log file *)
Common.set_logfile logfile;;

(* initialize the database *)
open PTreeDB;;
module ZSet = ZZp.Set;;

let timeout = !Settings.reconciliation_config_timeout;;

let settings = { (* copied from reconserver.ml *)
	mbar = !Settings.mbar;
	bitquantum = !Settings.bitquantum;
	treetype = (if !Settings.transactions
		then `transactional
		else if !Settings.disk_ptree 
		then `ondisk else `inmem);
	max_nodes = !Settings.max_ptree_nodes;
	dbdir = databasedir;
	cache_bytes = !Settings.ptree_cache_bytes;
	pagesize = !Settings.ptree_pagesize;
};;

init_db settings;;
init_ptree settings;;

(* tunnel functions *)
let rec tunnel_decode_rec cin cout =
	let message_length = input_byte cin in
	if (message_length>0) then
	(
		let message = String.create message_length in
		really_input cin message 0 message_length;
		output cout message 0 message_length;
		flush cout;
		tunnel_decode_rec cin cout
	);;

let rec tunnel_decode in_fd out_fd =
	let cin = Unix.in_channel_of_descr in_fd in
	let cout = Unix.out_channel_of_descr out_fd in
	tunnel_decode_rec cin cout;;

let rec tunnel_encode_rec cin cout =
	let data = String.create 255 in
	let len = input cin data 0 255 in
	output_byte cout len;
	output cout data 0 len;
	flush cout;
	if (len>0) then (
		tunnel_encode_rec cin cout
	);;

let rec tunnel_encode in_fd out_fd =
	let cin = Unix.in_channel_of_descr in_fd in
	let cout = Unix.out_channel_of_descr out_fd in
	tunnel_encode_rec cin cout;;

(* callbacks *)
let synchronize handler encoded_cin encoded_cout =
	(* create a channel decoded_cin that gets the decoded data from stdin *)
	let cin_read, cin_write = Unix.pipe() in
	let decoding_thread = Thread.create (fun () -> tunnel_decode encoded_cin cin_write) () in
	let decoded_cin = Unix.in_channel_of_descr cin_read in

	(* create a channel decoded_cout that accepts the decoded data, encodes it and sends it to stdout *)
	let cout_read, cout_write = Unix.pipe() in
	let encoding_thread = Thread.create (fun () -> tunnel_encode cout_read encoded_cout) () in
	let decoded_cout = Unix.out_channel_of_descr cout_write in

	(* convert channels to Channel objects *)
	let cin = (new Channel.sys_in_channel decoded_cin) in
	let cout = (new Channel.sys_out_channel decoded_cout) in

	(* synchronization *)
	let data = handler (get_ptree ()) cin cout in

	(* close connections *)
	close_in decoded_cin;
	close_out decoded_cout;

	Thread.join decoding_thread;
	Thread.join encoding_thread;

	(* process data *)
	ignore(data);;

while true do (
	let line = input_line stdin in
	match line with
	  "ADD" -> print_endline "OK"
	| "DELETE" -> print_endline "OK"
	| "SYNCHRONIZE_AS_SERVER" -> print_endline "OK"; synchronize Server.handle Unix.stdin Unix.stdout; print_endline "DONE"
	| "SYNCHRONIZE_AS_CLIENT" -> print_endline "OK"; synchronize Client.handle Unix.stdin Unix.stdout; print_endline "DONE"
	| "EXIT" -> closedb (); prerr_string databasedir; prerr_endline "->EXIT"; exit 0
	| _ -> print_endline "ERROR"
) done;;
