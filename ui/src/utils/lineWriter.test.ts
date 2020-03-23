import {mocked} from 'ts-jest/utils'

jest.mock('src/utils/ajax')
import {createLineFromModel, buildLineWriter, Precision} from 'src/utils/lineWriter'

import MockAjax from 'src/utils/ajax'

describe('creating a line from a model', () => {
  it('creates a line without tags', () => {
    const measurement = 'performance'
    const tags = {}
    const fields = {fps: 55}
    const timestamp = 1584990314

    const line = createLineFromModel(measurement, tags, fields, timestamp)
    expect(line).toBe('performance fps=55 1584990314')
  })

  it('creates a line without tags with multiple fields', () => {
    const measurement = 'performance'
    const tags = {}
    const fields = {fps: 49.33333, heap: 48577273}
    const timestamp = 1584990314

    const line = createLineFromModel(measurement, tags, fields, timestamp)
    expect(line).toBe('performance fps=49.33333,heap=48577273 1584990314')
  })

  it('creates a line with a tag', () => {
    const measurement = 'performance'
    const tags = {region: 'us-west'}
    const fields = {fps: 49.33333, heap: 48577273}
    const timestamp = 1584990314

    const line = createLineFromModel(measurement, tags, fields, timestamp)
    expect(line).toBe(
      'performance,region=us-west fps=49.33333,heap=48577273 1584990314'
    )
  })

  it('creates a line with multiple tags', () => {
    const measurement = 'performance'
    const tags = {region: 'us-west', status: 'good'}
    const fields = {fps: 49.33333, heap: 48577273}
    const timestamp = 1584990314

    const line = createLineFromModel(measurement, tags, fields, timestamp)
    expect(line).toBe(
      'performance,region=us-west,status=good fps=49.33333,heap=48577273 1584990314'
    )
  })

  it('alphabetizes tags by key, for write optimization', () => {
    const measurement = 'performance'
    const tags = {region: 'us-west', environment: 'dev'}
    const fields = {fps: 49.33333, heap: 48577273}
    const timestamp = 1584990314

    const line = createLineFromModel(measurement, tags, fields, timestamp)
    expect(line).toBe(
      'performance,environment=dev,region=us-west fps=49.33333,heap=48577273 1584990314'
    )
  })
})

describe('building a line writer', () => {
  const url = 'https://west-west-west.cloud2.influxdata.com'
  const orgId = '823723asdfc92134'
  const bucketName = 'performance'
  const authToken = 'asdf2#%JASjasg=='

  it('builds a line writer with the passed in properties', () => {
    const writeLine = buildLineWriter(url, orgId, bucketName, authToken)

    const measurement = 'performance'
    const tags = {region: 'us-west', environment: 'dev'}
    const fields = {fps: 55.583583242, timeInFrame: 15}
    const timestamp = 1585005787

    writeLine(measurement, tags, fields, Precision.s, timestamp)
    const [requestParams, shouldExcludeBasepath] = mocked(MockAjax).mock.calls[0]

    expect(shouldExcludeBasepath).toBeTruthy()
    expect(requestParams).toEqual({
      method: 'POST',
      url: `${url}/api/v2/write`,
      data: createLineFromModel(measurement, tags, fields, timestamp),
      headers: {
        Authorization: `Token ${authToken}`
      },
      params: {
        org: orgId,
        bucket: bucketName,
        precision: Precision.s
      }
    })
  })
})
