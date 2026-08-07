import { useEffect, useRef, useState } from 'react'
import { X, ChevronDown } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import {
  LOCALE,
  STRIP_DAYS,
  TIME_PRESETS,
  describeTimeFilter,
  formatDay,
  isSameDay,
  overlapsRange,
  presetLabel,
  resolveTimeFilter,
  type TimeFilter,
} from '@/utils/time-filter'

type TimeRangeFilterProps = {
  value: TimeFilter | null
  onChange: (value: TimeFilter | null) => void
  anchor: Date
}

const DAY_START = '00:00'
const DAY_END = '23:59'

const pad = (n: number) => String(n).padStart(2, '0')

function toTimeInput(date?: Date): string {
  return date ? `${pad(date.getHours())}:${pad(date.getMinutes())}` : ''
}

function startOfDay(date: Date): Date {
  const day = new Date(date)
  day.setHours(0, 0, 0, 0)
  return day
}

function addDays(date: Date, count: number): Date {
  const shifted = new Date(date)
  shifted.setDate(date.getDate() + count)
  return shifted
}

// The day strip picks the day, the time fields pick the time within it. The end
// bound is exclusive, so as an end 23:59 has to mean the very end of the day or a
// whole-day selection would drop its final minute.
function combine(day: Date, time: string, isEnd: boolean): string {
  const [hours, minutes] = time.split(':').map(Number)
  const endOfDay = isEnd && time === DAY_END
  const combined = new Date(day)
  combined.setHours(hours || 0, minutes || 0, endOfDay ? 59 : 0, endOfDay ? 999 : 0)
  return combined.toISOString()
}

function TimeField({
  label,
  value,
  onChange,
}: {
  label: string
  value: string
  onChange: (time: string) => void
}) {
  return (
    <label className="flex items-center gap-2">
      {label}
      <Input
        type="time"
        // Chrome lays the value box out as a flex child filling the input, so
        // centring what it shows takes more than text-align
        className="h-7 w-auto px-3 text-center [&::-webkit-calendar-picker-indicator]:hidden [&::-webkit-datetime-edit]:flex [&::-webkit-datetime-edit]:w-full [&::-webkit-datetime-edit]:justify-center"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    </label>
  )
}

export function TimeRangeFilter({ value, onChange, anchor }: TimeRangeFilterProps) {
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const onMouseDown = (e: MouseEvent) => {
      if (!containerRef.current?.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('mousedown', onMouseDown)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [open])

  // Show a relative filter as the range it currently resolves to; editing the
  // days or the time fields freezes it into that absolute range.
  const resolved = value ? resolveTimeFilter(value, anchor) : null
  const from = resolved?.start ? new Date(resolved.start) : undefined
  const to = resolved?.end ? new Date(resolved.end) : undefined

  const today = startOfDay(anchor)
  const days = Array.from({ length: STRIP_DAYS }, (_, i) =>
    addDays(today, i - (STRIP_DAYS - 1))
  )

  const coversDay = (day: Date) =>
    resolved ? overlapsRange(resolved, day, addDays(day, 1)) : false

  const emit = (start?: string, end?: string) => {
    // A start past the end matches nothing and nothing in the UI says why, so keep
    // the pair ordered instead (both are ISO strings, which sort chronologically)
    const ordered = start && end && start > end ? [end, start] : [start, end]
    onChange(
      ordered[0] || ordered[1]
        ? { kind: 'absolute', start: ordered[0], end: ordered[1] }
        : null
    )
  }

  const selectDay = (day: Date) => {
    // Times carry over between day picks, but a relative filter resolves to an
    // arbitrary "now", which makes a poor default for a whole day
    const absolute = value?.kind === 'absolute'
    let startTime = (absolute && toTimeInput(from)) || DAY_START
    let endTime = (absolute && toTimeInput(to)) || DAY_END

    // Extend when a single day is already picked, otherwise start a new selection
    const extending = absolute && from && to && isSameDay(from, to) && !isSameDay(from, day)
    if (extending) {
      const [first, last] = day < from ? [day, from] : [from, day]
      emit(combine(first, startTime, false), combine(last, endTime, true))
      return
    }
    // Re-picking the day the selection sits on widens it to the whole day, then
    // clears it, the way presets and chart bars toggle off
    const alreadyOnDay = from && to && isSameDay(from, day) && isSameDay(to, day)
    if (alreadyOnDay && startTime === DAY_START && endTime === DAY_END) {
      onChange(null)
      return
    }
    if (alreadyOnDay || startTime >= endTime) {
      startTime = DAY_START
      endTime = DAY_END
    }
    emit(combine(day, startTime, false), combine(day, endTime, true))
  }

  const handleTimeChange = (part: 'start' | 'end', time: string) => {
    // A time input reports "" for every incomplete value, which is every keystroke
    // until both fields are filled — dropping the bound then would clear the filter
    if (!time) return
    const isEnd = part === 'end'
    const day = (isEnd ? to : from) ?? from ?? to ?? anchor
    const bound = combine(day, time, isEnd)
    emit(isEnd ? resolved?.start : bound, isEnd ? bound : resolved?.end)
  }

  const selectPreset = (minutes: number) => {
    const isActive = value?.kind === 'relative' && value.minutes === minutes
    onChange(isActive ? null : { kind: 'relative', minutes })
  }

  return (
    <div
      ref={containerRef}
      className="relative inline-flex h-8 items-center rounded-md border bg-background text-xs font-light"
    >
      <button
        type="button"
        aria-expanded={open}
        aria-haspopup="dialog"
        onClick={() => setOpen(!open)}
        className={cn(
          'flex h-full cursor-pointer items-center gap-1 px-3 whitespace-nowrap',
          !value && 'text-gray-500'
        )}
      >
        {describeTimeFilter(value)}
        {!value && <ChevronDown className="h-3 w-3" />}
      </button>
      {value && (
        <button
          type="button"
          aria-label="Clear time filter"
          className="mr-2 cursor-pointer rounded-sm p-0.5 hover:bg-gray-200"
          onClick={() => {
            onChange(null)
            setOpen(false)
          }}
        >
          <X className="h-3 w-3" />
        </button>
      )}

      {open && (
        <div className="absolute top-full left-0 z-50 mt-1 w-max bg-white border rounded-md shadow-lg p-3">
          <div className="flex gap-1">
            {TIME_PRESETS.map((minutes) => (
              <Button
                key={minutes}
                variant={
                  value?.kind === 'relative' && value.minutes === minutes
                    ? 'outlineActive'
                    : 'outline'
                }
                size="sm"
                className="flex-1 text-xs font-light h-7 px-2"
                aria-pressed={value?.kind === 'relative' && value.minutes === minutes}
                onClick={() => selectPreset(minutes)}
              >
                {presetLabel(minutes)}
              </Button>
            ))}
          </div>

          <div className="my-3 border-t" />

          <div className="flex">
            {days.map((day, index) => {
              const selected = coversDay(day)
              const isFirst = selected && !(index > 0 && coversDay(days[index - 1]))
              const isLast =
                selected && !(index < days.length - 1 && coversDay(days[index + 1]))
              const isToday = index === days.length - 1
              return (
                <button
                  key={day.toISOString()}
                  type="button"
                  title={day.toLocaleDateString(LOCALE)}
                  aria-pressed={selected}
                  onClick={() => selectDay(day)}
                  className={cn(
                    'flex-auto cursor-pointer px-1.5 py-1.5 text-xs font-light whitespace-nowrap',
                    selected
                      ? cn(
                          isFirst || isLast ? 'bg-focus/70' : 'bg-focus/25',
                          isFirst && 'rounded-l-md',
                          isLast && 'rounded-r-md'
                        )
                      : 'rounded-md hover:bg-gray-100'
                  )}
                >
                  {isToday ? 'Today' : formatDay(day)}
                </button>
              )
            })}
          </div>

          <div className="mt-3 flex items-center justify-evenly text-xs font-light">
            <TimeField
              label="From"
              value={toTimeInput(from)}
              onChange={(time) => handleTimeChange('start', time)}
            />
            <TimeField
              label="until"
              value={toTimeInput(to)}
              onChange={(time) => handleTimeChange('end', time)}
            />
          </div>
        </div>
      )}
    </div>
  )
}
