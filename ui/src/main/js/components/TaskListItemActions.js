import React from 'react';
import { taskLogsPath } from 'pages/TaskLogs';

import { isGoProcess } from 'utils/Task';

export default function ({ task }) {
  return (<div className='task-list-item-host'>
    {isGoProcess(task.assignedTask.task)
      ? <a href={taskLogsPath(task.assignedTask.taskId)}>{task.assignedTask.slaveHost}</a>
      : <strong>{task.assignedTask.slaveHost}</strong>}
  </div>);
}
