let rec tunnel_decode_rec cin cout =
	let data = String.create 1024 in
	let len = input cin data 0 1024 in
	output_string cout "PACKET<";
	output cout data 0 len;
	output_string cout ">\n";
	flush cout;
	if (len>0) then(
		tunnel_decode_rec cin cout
	) else (
		output_string cout "END\n";
		flush cout;
	);;

let rec tunnel_decode in_fd out_fd =
	let cin = Unix.in_channel_of_descr in_fd in
	let cout = Unix.out_channel_of_descr out_fd in
	tunnel_decode_rec cin cout;;

(* let rec tunnel_encode_rec cin cout =
	let data = String.create 1024 in
	let len = input cin data 0 1024 in
	output cout data 0 len;
	flush cout;
	if (len>0) then(
		tunnel_encode_rec cin cout
	);;

let rec tunnel_encode in_fd out_fd =
	let cin = Unix.in_channel_of_descr in_fd in
	let cout = Unix.out_channel_of_descr out_fd in
	tunnel_encode_rec cin cout;; *)

(* create a channel cin that gets the decoded data from stdin *)
let cin_read, cin_write = Unix.pipe();;
let decoding_thread = Thread.create (fun () -> tunnel_decode Unix.stdin cin_write) ();;
let cin = Unix.in_channel_of_descr cin_read;;

(* read from this channel *)
while true do (
	let line = input_line cin in
	output_string stdout line;
	output_string stdout "\n";
	flush stdout;
	if (line="END") then (
		exit 0
	)
) done;;

Thread.join decoding_thread;;
