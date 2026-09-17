import React from 'react';

import Loading from 'components/Loading';

const CHUNK_SIZE = 65536;

export function taskLogsPath(taskId, stream = 'stdout') {
  return `/scheduler/logs/task/${encodeURIComponent(taskId)}/stream/${stream}`;
}

export default class TaskLogs extends React.Component {
  constructor(props) {
    super(props);
    this.state = {loading: true, error: null, data: null, response: null, offset: 0};
    this.requestGeneration = 0;
    this.mounted = false;
    this.load = this.load.bind(this);
  }

  componentDidMount() {
    this.mounted = true;
    this.load();
  }

  componentWillUnmount() {
    this.mounted = false;
  }

  componentDidUpdate(previousProps) {
    const previous = previousProps.match.params;
    const current = this.props.match.params;
    if (previous.taskId !== current.taskId || previous.stream !== current.stream) {
      this.load();
    }
  }

  load(offset = 0) {
    const {taskId, stream} = this.props.match.params;
    const generation = ++this.requestGeneration;
    if (stream !== 'stdout' && stream !== 'stderr') {
      this.setState({loading: false, error: 'Log stream must be stdout or stderr.', data: null});
      return;
    }
    this.setState({loading: true, error: null, data: null, offset});
    window.fetch(
      `/tasklogs/${encodeURIComponent(taskId)}/${stream}?offset=${offset}&limit=${CHUNK_SIZE}`)
      .then((response) => response.json().then((body) => {
        if (!response.ok) {
          throw new Error(body.error || body.message || `Unable to load ${stream} logs.`);
        }
        return body;
      }))
      .then((body) => {
        if (this.mounted && generation === this.requestGeneration) {
          this.setState({loading: false, data: body.data || '', response: body, offset});
        }
      })
      .catch((error) => {
        if (this.mounted && generation === this.requestGeneration) {
          this.setState({loading: false, error: error.message, data: null});
        }
      });
  }

  render() {
    const {taskId, stream} = this.props.match.params;
    const {loading, error, data, response, offset} = this.state;
    if (loading) {
      return <Loading />;
    }
    const streams = (<p className='task-logs-streams'>
      <a
        className={stream === 'stdout' ? 'active' : ''}
        href={taskLogsPath(taskId, 'stdout')}>stdout</a>
      {' | '}
      <a
        className={stream === 'stderr' ? 'active' : ''}
        href={taskLogsPath(taskId, 'stderr')}>stderr</a>
    </p>);
    if (error) {
      return (<div className='task-logs'>
        <h2>Task logs</h2>
        {streams}
        <p className='text-danger'>{error}</p>
        <button onClick={() => this.load(offset)}>Retry</button>
      </div>);
    }

    return (<div className='task-logs'>
      <h2>Task logs</h2>
      <p><strong>Task ID:</strong> <span className='debug-data'>{taskId}</span></p>
      {streams}
      <pre className='task-logs-output'>{data || 'No output retained for this stream.'}</pre>
      {response && response.truncated &&
        <p>
          Output retained up to the agent log limit; truncation is confirmed when the task exits.
        </p>}
      {response && response.complete === false &&
        <p>More output may be available. Refresh to check again.</p>}
      <button onClick={() => this.load(0)}>Restart</button>
      {' '}
      <button onClick={() => this.load(offset)}>Refresh</button>
      {' '}
      <button
        disabled={!response || !response.hasMore || response.nextOffset <= offset}
        onClick={() => this.load(response.nextOffset)}>
        Next
      </button>
    </div>);
  }
}
