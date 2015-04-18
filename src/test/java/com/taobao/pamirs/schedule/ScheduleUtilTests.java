package com.taobao.pamirs.schedule;

import org.junit.Test;

import static com.taobao.pamirs.schedule.ScheduleUtil.assignTaskNumber;

public class ScheduleUtilTests {
	
	private static String printArray(int[] items) {
		String s = "";
		for (int i = 0; i < items.length; i++) {
			if (i > 0) {
				s = s + ",";
			}
			s = s + items[i];
		}
		return s;
	}

	@Test
	public void testAll() {
		System.out.println(printArray(assignTaskNumber(1, 10, 0)));
		System.out.println(printArray(assignTaskNumber(2, 10, 0)));
		System.out.println(printArray(assignTaskNumber(3, 10, 0)));
		System.out.println(printArray(assignTaskNumber(4, 10, 0)));
		System.out.println(printArray(assignTaskNumber(5, 10, 0)));
		System.out.println(printArray(assignTaskNumber(6, 10, 0)));
		System.out.println(printArray(assignTaskNumber(7, 10, 0)));
		System.out.println(printArray(assignTaskNumber(8, 10, 0)));
		System.out.println(printArray(assignTaskNumber(9, 10, 0)));
		System.out.println(printArray(assignTaskNumber(10, 10, 0)));

		System.out.println("-----------------");

		System.out.println(printArray(assignTaskNumber(1, 10, 3)));
		System.out.println(printArray(assignTaskNumber(2, 10, 3)));
		System.out.println(printArray(assignTaskNumber(3, 10, 3)));
		System.out.println(printArray(assignTaskNumber(4, 10, 3)));
		System.out.println(printArray(assignTaskNumber(5, 10, 3)));
		System.out.println(printArray(assignTaskNumber(6, 10, 3)));
		System.out.println(printArray(assignTaskNumber(7, 10, 3)));
		System.out.println(printArray(assignTaskNumber(8, 10, 3)));
		System.out.println(printArray(assignTaskNumber(9, 10, 3)));
		System.out.println(printArray(assignTaskNumber(10, 10, 3)));
	}
}
