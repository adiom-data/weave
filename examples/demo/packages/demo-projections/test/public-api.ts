import { ProjectDetails, type ProjectDetailsData } from '../dist/index.js'
import type { ReactiveClient } from '@adiom-data/weave'

ProjectDetails.request({ projectId: '10000000-0000-4000-8000-000000000001' })
// @ts-expect-error UUID parameters are not numbers
ProjectDetails.request({ projectId: 1 })
// @ts-expect-error required parameter cannot be omitted
ProjectDetails.request({})

function check(client: ReactiveClient, data: ProjectDetailsData): void {
  const handle = ProjectDetails.subscribe(client, { projectId: 'id' })
  if (data.status !== 'available') {
    // @ts-expect-error pending/unavailable entities do not carry stale payloads
    data.value
    return
  }
  const list = data.value.taskLinks
  if (!list) return
  void handle.window(list).extend('RIGHT', 20)
  const bounds = handle.window(list).bounds.getSnapshot()
  if (bounds?.left?.cursor) void handle.window(list).move('LEFT', bounds.left.cursor)
  // @ts-expect-error window bounds are a read-only selection
  handle.window(list).bounds.stage({})
  // @ts-expect-error plain arrays are not projection-owned lists
  handle.window([])
  const link = list[0]
  if (link?.status !== 'available') return
  const position: string = link.value.position
  void position
  // @ts-expect-error title was selected on Task, not ProjectTask
  link.value.title
  if (link.value.task?.status === 'available') {
    const completed: boolean = link.value.task.value.completed
    void completed
    // @ts-expect-error schema fields that weren't selected are absent
    link.value.task.value.organizationId
  }
}
void check
