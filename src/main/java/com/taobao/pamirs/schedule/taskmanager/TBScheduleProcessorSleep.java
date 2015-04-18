package com.taobao.pamirs.schedule.taskmanager;

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.pamirs.schedule.IScheduleTaskDeal;
import com.taobao.pamirs.schedule.IScheduleTaskDealMulti;
import com.taobao.pamirs.schedule.IScheduleTaskDealSingle;
import com.taobao.pamirs.schedule.TaskItemDefine;

/**
 * 任务调度器，在TBScheduleManager的管理下实现多线程数据处理
 * 
 * @author xuannan
 *
 * @param <T>
 */
class TBScheduleProcessorSleep<T> implements IScheduleProcessor, Runnable {

	private static transient Logger logger = LoggerFactory.getLogger(TBScheduleProcessorSleep.class);
	
	final LockObject m_lockObject = new LockObject();
	List<Thread> threadList = Collections.synchronizedList(new ArrayList<Thread>());
	/**
	 * 任务管理器
	 */
	protected TBScheduleManager scheduleManager;
	/**
	 * 任务类型
	 */
	ScheduleTaskType taskTypeInfo;

	/**
	 * 任务处理的接口类
	 */
	protected IScheduleTaskDeal<T> taskDealBean;

	/**
	 * 当前任务队列的版本号
	 */
	protected long taskListVersion = 0;
	final Object lockVersionObject = new Object();
	final Object lockRunningList = new Object();

	protected List<T> taskList = Collections.synchronizedList(new ArrayList<T>());

	/**
	 * 是否可以批处理
	 */
	boolean isMutilTask = false;

	/**
	 * 是否已经获得终止调度信号
	 */
	boolean isStopSchedule = false;// 用户停止队列调度
	boolean isSleeping = false;

	StatisticsInfo statisticsInfo;

	//wangxiaohu add job日志id
	private Long taskDomainId = null;
	//获取到记录数量
	private Integer taskGetListSize = 0;
	//执行完毕线程list集合
	private List<String> overThreadList = new ArrayList<String>();
	private boolean isSkipGetData = false;

	/**
	 * 创建一个调度处理器
	 * 
	 * @param aManager
	 * @param aTaskDealBean
	 * @param aStatisticsInfo
	 * @throws Exception
	 */
	public TBScheduleProcessorSleep(TBScheduleManager aManager,
			IScheduleTaskDeal<T> aTaskDealBean, StatisticsInfo aStatisticsInfo)
			throws Exception {
		this.scheduleManager = aManager;
		this.statisticsInfo = aStatisticsInfo;
		this.taskTypeInfo = this.scheduleManager.getTaskTypeInfo();
		this.taskDealBean = aTaskDealBean;
		if (this.taskDealBean instanceof IScheduleTaskDealSingle<?>) {
			if (taskTypeInfo.getExecuteNumber() > 1) {
				taskTypeInfo.setExecuteNumber(1);
			}
			isMutilTask = false;
		} else {
			isMutilTask = true;
		}
		if (taskTypeInfo.getFetchDataNumber() < taskTypeInfo.getThreadNumber() * 10) {
			logger.warn("参数设置不合理，系统性能不佳。【每次从数据库获取的数量fetchnum】 >= 【线程数量threadnum】 *【最少循环次数10】 ");
		}
		for (int i = 0; i < taskTypeInfo.getThreadNumber(); i++) {
			this.startThread(i);
		}
	}

	/**
	 * 需要注意的是，调度服务器从配置中心注销的工作，必须在所有线程退出的情况下才能做
	 * 
	 * @throws Exception
	 */
	@Override
	public void stopSchedule() throws Exception {
		// 设置停止调度的标志,调度线程发现这个标志，执行完当前任务后，就退出调度
		this.isStopSchedule = true;
		//清除所有未处理任务,但已经进入处理队列的，需要处理完毕
		this.taskList.clear();
	}

	private void startThread(int index) {
		Thread thread = new Thread(this);
		threadList.add(thread);
		String threadName = this.scheduleManager.getScheduleServer()
				.getTaskType()
				+ "-"
				+ this.scheduleManager.getCurrentSerialNumber()
				+ "-exe"
				+ index;
		thread.setName(threadName);
		thread.start();
	}

	public synchronized Object getScheduleTaskId() {
		if (this.taskList.size() > 0)
			return this.taskList.remove(0); // 按正序处理
		return null;
	}

	public synchronized Object[] getScheduleTaskIdMulti() {
		if (this.taskList.size() == 0) {
			return null;
		}
		int size = taskList.size() > taskTypeInfo.getExecuteNumber() ? taskTypeInfo
				.getExecuteNumber() : taskList.size();

		Object[] result = null;
		if (size > 0) {
			result = (Object[]) Array.newInstance(this.taskList.get(0)
					.getClass(), size);
		}
		for (int i = 0; i < size; i++) {
			result[i] = this.taskList.remove(0); // 按正序处理
		}
		return result;
	}

	@Override
	public void clearAllHasFetchData() {
		this.taskList.clear();
	}

	@Override
	public boolean isDealFinishAllData() {
		return this.taskList.size() == 0;
	}

	@Override
	public boolean isSleeping() {
		return this.isSleeping;
	}

	protected int loadScheduleData() {
		try {
			//在每次数据处理完毕后休眠固定的时间
			if (this.taskTypeInfo.getSleepTimeInterval() > 0) {
				if (logger.isDebugEnabled()) {
					logger.debug("处理完一批数据后休眠："
							+ this.taskTypeInfo.getSleepTimeInterval());
				}
				this.isSleeping = true;
				Thread.sleep(taskTypeInfo.getSleepTimeInterval());
				this.isSleeping = false;

				if (logger.isDebugEnabled()) {
					logger.debug("处理完一批数据后休眠后恢复");
				}
			} else {
				if (isSkipGetData) {
					return 0;
				}
			}

			List<TaskItemDefine> taskItems = this.scheduleManager
					.getCurrentScheduleTaskItemList();
			// 根据队列信息查询需要调度的数据，然后增加到任务列表中
			if (taskItems.size() > 0) {
				if (this.taskList.isEmpty()) {
					this.overThreadList.clear();
					Long taskDomainId = taskDealBean.beforeTask(taskDealBean
							.getClass().getName(), taskTypeInfo
							.getTaskParameter(), scheduleManager
							.getScheduleServer().getOwnSign(),
							this.scheduleManager.getTaskItemCount(), taskItems,
							taskTypeInfo.getFetchDataNumber());
					setTaskDomainId(taskDomainId);
				}
				List<T> tmpList = this.taskDealBean.selectTasks(taskTypeInfo
						.getTaskParameter(), scheduleManager
						.getScheduleServer().getOwnSign(), this.scheduleManager
						.getTaskItemCount(), taskItems, taskTypeInfo
						.getFetchDataNumber());
				taskGetListSize = tmpList == null ? 0 : tmpList.size();
				
				scheduleManager.getScheduleServer().setLastFetchDataTime(
						new Timestamp(scheduleManager.scheduleCenter
								.getSystemTime()));
				if (tmpList != null) {
					this.taskList.addAll(tmpList);
				}
			} else {
				logger.debug("没有获取到需要处理的数据队列");
			}
			addFetchNum(taskList.size(), "TBScheduleProcessor.loadScheduleData");
			return this.taskList.size();
		} catch (Throwable ex) {
			logger.error("Get tasks error.", ex);
		}
		return 0;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void run() {
		try {
			long startTime = 0;
			while (true) {
				this.m_lockObject.addThread();
				Object executeTask;
				//wangxiaohu  添加quatz运行方式
				int listSize = this.taskList.size();
				//wangxiaohu end
				while (true) {
					if (this.isStopSchedule) {//停止队列调度
						this.m_lockObject.realseThread();
						this.m_lockObject.notifyOtherThread();//通知所有的休眠线程
						synchronized (this.threadList) {
							try {
								this.threadList.remove(Thread.currentThread());
								if (this.threadList.isEmpty()) {
									this.scheduleManager.unRegisterScheduleServer();
								}
							} catch (Exception e) {
								afterTask();
								throw new Exception("清理job发生异常，剩余 "
										+ this.taskList.size() + " 条数据没有被执行!",
										e);
							}
						}
						return;
					}

					//加载调度任务
					if (!this.isMutilTask) {
						executeTask = this.getScheduleTaskId();
					} else {
						executeTask = this.getScheduleTaskIdMulti();
					}

					if (executeTask == null) {
						break;
					}

					try {//运行相关的程序
						startTime = scheduleManager.scheduleCenter.getSystemTime();
						
						if (!this.isMutilTask) {
							if (((IScheduleTaskDealSingle) this.taskDealBean).execute(executeTask, scheduleManager.getScheduleServer().getOwnSign())) {
								addSuccessNum(
										1,
										scheduleManager.scheduleCenter.getSystemTime() - startTime,
										"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
							} else {
								addFailNum(
										1,
										scheduleManager.scheduleCenter.getSystemTime() - startTime,
										"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
							}
						} else {
							if (((IScheduleTaskDealMulti) this.taskDealBean).execute((Object[]) executeTask, scheduleManager.getScheduleServer().getOwnSign())) {
								addSuccessNum(
										((Object[]) executeTask).length,
										scheduleManager.scheduleCenter.getSystemTime() - startTime,
										"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
							} else {
								addFailNum(
										((Object[]) executeTask).length,
										scheduleManager.scheduleCenter.getSystemTime() - startTime,
										"com.taobao.pamirs.schedule.TBScheduleProcessorSleep.run");
							}
						}
					} catch (Exception ex) {
						if (this.isMutilTask == false) {
							addFailNum(
									1,
									scheduleManager.scheduleCenter.getSystemTime() - startTime,
									"TBScheduleProcessor.run");
						} else {
							addFailNum(
									((Object[]) executeTask).length,
									scheduleManager.scheduleCenter
											.getSystemTime() - startTime,
									"TBScheduleProcessor.run");
						}
						logger.warn("Task :" + executeTask + " 处理失败", ex);
						if (getTaskDomainId() != null) {
							taskDealBean.onException(getTaskDomainId(), ex);
						}
					}
				}
				//当前队列中所有的任务都已经完成了。
				if (logger.isDebugEnabled()) {
					logger.debug(Thread.currentThread().getName()
							+ "：当前运行线程数量:" + this.m_lockObject.count());
				}
				if (!this.m_lockObject.realseThreadButNotLast()) {
					//wangxiaohu  添加quatz运行方式
					if (listSize != 0) {
						isSkipGetData = true;
						if (this.taskTypeInfo.getSleepTimeInterval() < 0) {
							this.scheduleManager.pause("没数据休眠时间过短，使用quatz运行方式！");
						}
						afterTask();
					}
					//wangxiaohu end
					int size = 0;
					Thread.currentThread();
					Thread.sleep(100);
					startTime = scheduleManager.scheduleCenter.getSystemTime();
					// 装载数据
					size = this.loadScheduleData();
					if (size > 0) {
						this.m_lockObject.notifyOtherThread();
					} else {
						//判断当没有数据的是否，是否需要退出调度
						if (!this.isStopSchedule && this.scheduleManager.isContinueWhenData()) {
							if (logger.isDebugEnabled()) {
								logger.debug("没有装载到数据，start sleep");
							}
							this.isSleeping = true;
							Thread.currentThread();
							Thread.sleep(this.scheduleManager.getTaskTypeInfo().getSleepTimeNoData());
							this.isSleeping = false;

							if (logger.isDebugEnabled()) {
								logger.debug("Sleep end");
							}
						} else {
							//没有数据，退出调度，唤醒所有沉睡线程
							this.m_lockObject.notifyOtherThread();
						}
						afterTask();
					}
					this.m_lockObject.realseThread();
				} else {// 将当前线程放置到等待队列中。直到有线程装载到了新的任务数据
					if (logger.isDebugEnabled()) {
						logger.debug("不是最后一个线程，sleep");
					}
					this.m_lockObject.waitCurrentThread();
				}
			}
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void afterTask() {
		if (getTaskDomainId() != null
				&& !overThreadList.contains(Thread.currentThread().getName())) {
			overThreadList.add(Thread.currentThread().getName());
			taskDealBean.afterTask(getTaskDomainId(), taskGetListSize);
		}
	}

	private void setTaskDomainId(Long taskDomainId) {
		this.taskDomainId = taskDomainId;
	}

	private Long getTaskDomainId() {
		return this.taskDomainId;
	}

	public void addFetchNum(long num, String addr) {
		this.statisticsInfo.addFetchDataCount(1);
		this.statisticsInfo.addFetchDataNum(num);
	}

	public void addSuccessNum(long num, long spendTime, String addr) {
		this.statisticsInfo.addDealDataSucess(num);
		this.statisticsInfo.addDealSpendTime(spendTime);
	}

	public void addFailNum(long num, long spendTime, String addr) {
		this.statisticsInfo.addDealDataFail(num);
		this.statisticsInfo.addDealSpendTime(spendTime);
	}
}
