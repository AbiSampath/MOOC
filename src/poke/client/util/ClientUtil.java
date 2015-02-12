package poke.client.util;

import poke.util.PrintNode;
import eye.Comm.Header;
import eye.Comm.JobDesc;
import eye.Comm.NameValueSet;
import eye.Comm.Ping;

public class ClientUtil {

	public static void printJob(JobDesc job) {
		if (job == null) {
			System.out.println("job is null");
			return;
		}

		if (job.hasNameSpace())
			System.out.println("NameSpace: " + job.getNameSpace());

		if (job.hasJobId()) {
		}

		if (job.hasStatus()) {
			System.out.println("Status:    " + job.getStatus());
		}

		if (job.hasOptions()) {
			NameValueSet nvs = job.getOptions();
			PrintNode.print(nvs);
		}
	}

	public static void printPing(Ping f) {
		if (f == null) {
			System.out.println("ping is null");
			return;
		}

		System.out.println("Poke: " + f.getTag() + " - " + f.getNumber());
	}

	public static void printHeader(Header h) {
		System.out.println("-------------------------------------------------------");
		System.out.println("Header");
		System.out.println(" - Orig   : " + h.getOriginator());
		System.out.println(" - Req ID : " + h.getRoutingId());
		System.out.println(" - Tag    : " + h.getTag());
		System.out.println(" - Time   : " + h.getTime());
		System.out.println(" - Status : " + h.getReplyCode());
		if (h.getReplyCode().getNumber() != eye.Comm.PokeStatus.SUCCESS_VALUE)
			System.out.println(" - Re Msg : " + h.getReplyMsg());

		System.out.println("");
	}

}
