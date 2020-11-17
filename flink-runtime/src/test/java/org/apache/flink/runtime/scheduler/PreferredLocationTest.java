package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.net.InetAddress;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PreferredLocationTest extends TestLogger {

	@Test
	public void assignPreferredLocation() throws Exception{
		ResourceID resourceID1 = new ResourceID("a");
		ResourceID resourceID2 = new ResourceID("b");
		ResourceID resourceID3 = new ResourceID("c");

		InetAddress address1 = mock(InetAddress.class);
		when(address1.getCanonicalHostName()).thenReturn("localhost");
		when(address1.getHostName()).thenReturn("localhost");
		when(address1.getHostAddress()).thenReturn("127.0.0.1");
		when(address1.getAddress()).thenReturn(new byte[] {127, 0, 0, 1} );

		InetAddress address2 = mock(InetAddress.class);
		when(address2.getCanonicalHostName()).thenReturn("testhost1");
		when(address2.getHostName()).thenReturn("testhost1");
		when(address2.getHostAddress()).thenReturn("0.0.0.0");
		when(address2.getAddress()).thenReturn(new byte[] {0, 0, 0, 0} );

		InetAddress address3 = mock(InetAddress.class);
		when(address3.getCanonicalHostName()).thenReturn("testhost2");
		when(address3.getHostName()).thenReturn("testhost2");
		when(address3.getHostAddress()).thenReturn("192.168.0.1");
		when(address3.getAddress()).thenReturn(new byte[] {(byte) 192, (byte) 168, 0, 1} );

		TaskManagerLocation one = new TaskManagerLocation(resourceID1, address1, 19871);
		TaskManagerLocation two = new TaskManagerLocation(resourceID2, address2, 19871);
		TaskManagerLocation three = new TaskManagerLocation(resourceID3, address3, 10871);
	}
}
