import React from 'react';
import { shallow } from 'enzyme';
import { matchPath } from 'react-router-dom';

import TaskLogs, { taskLogsPath } from '../TaskLogs';

describe('TaskLogs', () => {
  it('does not collide with scheduler job, instance, task or update routes', () => {
    const path = taskLogsPath('task-id', 'stdout');
    ['/scheduler/:role/:environment/:name', '/scheduler/:role/:environment/:name/:instance',
      '/scheduler/:role/:environment/:name/task/:taskId',
      '/scheduler/:role/:environment/:name/update/:uid'].forEach((pattern) => {
      expect(matchPath(path, {path: pattern, exact: true})).toBeNull();
    });
    expect(matchPath(path, {
      path: '/scheduler/logs/task/:taskId/stream/:stream', exact: true
    }).params).toEqual({taskId: 'task-id', stream: 'stdout'});
  });
  it('builds a scheduler-local encoded log URL', () => {
    expect(taskLogsPath('task/id', 'stderr')).toBe('/scheduler/logs/task/task%2Fid/stream/stderr');
  });

  it('renders retained output and truncation information', () => {
    const wrapper = shallow(
      <TaskLogs match={{params: {taskId: 'task-id', stream: 'stdout'}}} />,
      {disableLifecycleMethods: true});
    wrapper.setState({
      loading: false,
      data: 'hello\n',
      response: {truncated: true, complete: true},
      error: null
    });
    expect(wrapper.find('pre.task-logs-output').text()).toBe('hello\n');
    expect(wrapper.text()).toContain('truncation is confirmed');
  });

  it('requests the next bounded page when next output is available', () => {
    const wrapper = shallow(
      <TaskLogs match={{params: {taskId: 'task-id', stream: 'stdout'}}} />,
      {disableLifecycleMethods: true});
    wrapper.setState({
      loading: false,
      data: 'page',
      offset: 0,
      response: {hasMore: true, nextOffset: 65536},
      error: null
    });
    wrapper.instance().load = jest.fn();
    wrapper.find('button').filterWhere((button) => button.text() === 'Next').simulate('click');
    expect(wrapper.instance().load).toHaveBeenCalledWith(65536);
  });

  it('ignores a response from an older task or stream request', async () => {
    const originalFetch = global.fetch;
    const pending = [];
    global.fetch = jest.fn(() => new Promise((resolve) => pending.push(resolve)));
    const page = (data) => ({ok: true, json: () => Promise.resolve({data, hasMore: false})});
    const wrapper = shallow(<TaskLogs match={{params: {taskId: 'first', stream: 'stdout'}}} />);
    const instance = wrapper.instance();
    wrapper.setProps({match: {params: {taskId: 'second', stream: 'stderr'}}});
    pending[1](page('newer'));
    await Promise.resolve();
    await Promise.resolve();
    pending[0](page('older'));
    await Promise.resolve();
    await Promise.resolve();
    expect(instance.state.data).toBe('newer');
    wrapper.unmount();
    global.fetch = originalFetch;
  });
});
