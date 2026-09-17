import React from 'react';
import { shallow } from 'enzyme';

import TaskListItemActions from '../TaskListItemActions';
import {
  AssignedTaskBuilder,
  ScheduledTaskBuilder,
  TaskConfigBuilder
} from 'test-utils/TaskBuilders';

describe('TaskListItemActions', () => {
  it('links every supported executor task to scheduler logs', () => {
    const config = TaskConfigBuilder.executorConfig({name: 'go-process'}).build();
    const task = ScheduledTaskBuilder.assignedTask(
      AssignedTaskBuilder.task(config).build()).build();
    const link = shallow(<TaskListItemActions task={task} />).find('a');
    expect(link.prop('href')).toBe('/scheduler/logs/task/test-task-id/stream/stdout');
  });
});
