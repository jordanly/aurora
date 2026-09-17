import React from 'react';
import { taskLogsPath } from 'pages/TaskLogs';
import { isGoProcess } from 'utils/Task';

export default function TaskDetails({ task }) {
  return (<div className='active-task-details'>
    <div>
      <h5>Task ID</h5>
      <span className='debug-data'>{task.assignedTask.taskId}</span>
      <a href={`/structdump/task/${task.assignedTask.taskId}`}>view raw config</a>
    </div>
    {task.assignedTask.slaveHost ? <div className='active-task-details-host'>
      <h5>Host</h5>
      <span className='debug-data'>{task.assignedTask.slaveHost}</span>
      {isGoProcess(task.assignedTask.task)
        ? <a href={taskLogsPath(task.assignedTask.taskId)}>view logs</a>
        : null}
    </div> : null}
  </div>);
}
