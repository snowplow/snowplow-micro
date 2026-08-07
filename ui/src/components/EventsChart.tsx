import { useMemo, useState } from 'react'
import { Bar, BarChart, XAxis, Cell } from 'recharts'
import {
  type ChartConfig,
  ChartContainer,
  ChartTooltip,
} from '@/components/ui/chart'
import { type TimelineData } from '@/services/api'
import { overlapsRange, resolveTimeFilter, type TimeFilter } from '@/utils/time-filter'

interface EventsChartProps {
  timelineData: TimelineData
  timeFilter: TimeFilter | null
  onTimeFilterChange: (value: TimeFilter | null) => void
  timeFilterAnchor: Date
  timeFormat: (date: Date) => string
}

// ISO strings can differ in formatting (e.g. with/without milliseconds), so compare as instants
const sameInstant = (a?: string, b?: string): boolean =>
  a && b ? new Date(a).getTime() === new Date(b).getTime() : !a && !b

const chartConfig = {
  validEvents: {
    label: 'Valid Events',
    color: 'rgb(var(--dark-teal))',
  },
  failedEvents: {
    label: 'Failed Events',
    color: 'rgb(var(--dark-orange))',
  },
} satisfies ChartConfig

export function EventsChart({
  timelineData,
  timeFilter,
  onTimeFilterChange,
  timeFilterAnchor,
  timeFormat,
}: EventsChartProps) {
  const [barHovered, setBarHovered] = useState(false)

  const chartData = useMemo(() => {
    return timelineData.points.map(point => ({
      label: timeFormat(new Date(point.bucket.start)),
      validEvents: point.validEvents,
      failedEvents: point.failedEvents,
      bucketStart: point.bucket.start,
      bucketEnd: point.bucket.end,
    }))
  }, [timelineData, timeFormat])

  const handleChartClick = (event: any) => {
    if (event && event.activePayload && event.activePayload.length > 0) {
      const data = event.activePayload[0].payload

      // Check if we actually have events at this time point
      if (data.validEvents > 0 || data.failedEvents > 0) {
        // Clicking on a bar with actual data - toggle selection
        const isSelected =
          timeFilter?.kind === 'absolute' &&
          sameInstant(timeFilter.start, data.bucketStart) &&
          sameInstant(timeFilter.end, data.bucketEnd)
        onTimeFilterChange(
          isSelected
            ? null
            : { kind: 'absolute', start: data.bucketStart, end: data.bucketEnd }
        )
      } else {
        // Clicking on empty area (no events at this time) - reset selection
        onTimeFilterChange(null)
      }
    } else {
      // Clicking on empty area - reset selection
      onTimeFilterChange(null)
    }
  }

  const selectedRange = useMemo(
    () => (timeFilter ? resolveTimeFilter(timeFilter, timeFilterAnchor) : null),
    [timeFilter, timeFilterAnchor]
  )

  const getBarOpacity = (index: number): number => {
    if (!selectedRange) return 0.8
    const data = chartData[index]
    const overlaps = overlapsRange(
      selectedRange,
      new Date(data.bucketStart),
      new Date(data.bucketEnd)
    )
    return overlaps ? 1.0 : 0.2
  }

  return (
    <div className="h-[100px] w-full rounded-md border bg-background">
      <ChartContainer config={chartConfig} className="h-full w-full">
        <BarChart
          height={100}
          data={chartData}
          onClick={handleChartClick}
          margin={{ top: 10, right: 10, left: 10, bottom: 0 }}
        >
          <defs>
            <pattern
              id="failedPattern"
              patternUnits="userSpaceOnUse"
              width="6"
              height="6"
            >
              <rect width="6" height="6" fill="var(--color-failure)" />
              <line
                x1="0"
                y1="0"
                x2="6"
                y2="6"
                stroke="var(--color-failure-dark)"
                strokeWidth="0.5"
                opacity="0.6"
              />
              <line
                x1="0"
                y1="6"
                x2="6"
                y2="0"
                stroke="var(--color-failure-dark)"
                strokeWidth="0.5"
                opacity="0.6"
              />
            </pattern>
          </defs>
          <XAxis
            dataKey="label"
            axisLine={false}
            tickLine={false}
            fontSize={12}
          />
          <ChartTooltip
            wrapperStyle={{zIndex: 20}}
            content={({ active, payload }) => {
              if (active && payload && payload.length > 0 && barHovered) {
                const data = payload[0].payload
                const startTime = new Date(data.bucketStart)
                const endTime = new Date(data.bucketEnd)
                return (
                  <div className="bg-gray-900 text-white px-3 py-2 rounded shadow-lg text-sm">
                    <div className="font-medium mb-1">
                      {startTime.toLocaleString()} - {endTime.toLocaleTimeString()}
                    </div>
                    <div className="space-y-1">
                      {data.validEvents > 0 && (
                        <div>Valid Events: {data.validEvents}</div>
                      )}
                      {data.failedEvents > 0 && (
                        <div>Failed Events: {data.failedEvents}</div>
                      )}
                    </div>
                    <div className="text-xs text-gray-300 mt-2 border-t border-gray-600 pt-1">
                      Click to toggle filter
                    </div>
                  </div>
                )
              }
              return null
            }}
            cursor={false}
            animationDuration={0}
            isAnimationActive={false}
          />
          <Bar
            dataKey="validEvents"
            stackId="events"
            fill="var(--color-success)"
            onMouseEnter={() => setBarHovered(true)}
            onMouseLeave={() => setBarHovered(false)}
            className="cursor-pointer"
            isAnimationActive={false}
          >
            {chartData.map((entry, index) => {
              const hasFailedEvents = entry.failedEvents > 0
              const radius = hasFailedEvents ? [0, 0, 2, 2] : [2, 2, 2, 2]
              return (
                <Cell
                  key={`valid-cell-${index}`}
                  fillOpacity={getBarOpacity(index)}
                  // @ts-ignore https://github.com/recharts/recharts/issues/3325
                  radius={radius}
                />
              )
            })}
          </Bar>
          <Bar
            dataKey="failedEvents"
            stackId="events"
            fill="url(#failedPattern)"
            onMouseEnter={() => setBarHovered(true)}
            onMouseLeave={() => setBarHovered(false)}
            className="cursor-pointer"
            isAnimationActive={false}
          >
            {chartData.map((entry, index) => {
              const hasValidEvents = entry.validEvents > 0
              const radius = hasValidEvents ? [2, 2, 0, 0] : [2, 2, 2, 2]
              return (
                <Cell
                  key={`failed-cell-${index}`}
                  fillOpacity={getBarOpacity(index)}
                  // @ts-ignore https://github.com/recharts/recharts/issues/3325
                  radius={radius}
                />
              )
            })}
          </Bar>
        </BarChart>
      </ChartContainer>
    </div>
  )
}
