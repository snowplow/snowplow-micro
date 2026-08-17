// @vitest-environment jsdom
import { useState } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import '@testing-library/jest-dom/vitest'
import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { TimeRangeFilter } from './TimeRangeFilter'
import { type TimeFilter } from '@/utils/time-filter'

afterEach(cleanup)

const localWallClock = (
  year: number,
  month: number,
  day: number,
  hours = 0,
  minutes = 0,
  seconds = 0,
  ms = 0
) => new Date(year, month - 1, day, hours, minutes, seconds, ms)

const ANCHOR = localWallClock(2026, 3, 14, 12, 0)

const wholeDay = (day: number) => ({
  kind: 'absolute' as const,
  start: localWallClock(2026, 3, day).toISOString(),
  end: localWallClock(2026, 3, day, 23, 59, 59, 999).toISOString(),
})

function renderFilter(initial: TimeFilter | null = null) {
  const onChange = vi.fn()
  function Harness() {
    const [value, setValue] = useState(initial)
    return (
      <TimeRangeFilter
        value={value}
        anchor={ANCHOR}
        onChange={(next) => {
          onChange(next)
          setValue(next)
        }}
      />
    )
  }
  render(<Harness />)
  return onChange
}

const trigger = () => screen.getByRole('button', { expanded: false })
const open = () => fireEvent.click(trigger())
const day = (label: string) => screen.getByRole('button', { name: label })
const preset = (minutes: number) =>
  screen
    .getAllByRole('button', { name: `Last ${minutes} min` })
    .filter((button) => button.hasAttribute('aria-pressed'))[0]
const from = () => screen.getByLabelText('From')
const until = () => screen.getByLabelText('until')
const isOpen = () => screen.queryByLabelText('From') !== null
const highlightedDays = () =>
  screen.queryAllByRole('button', { pressed: true }).map((button) => button.textContent)

describe('TimeRangeFilter', () => {
  it('reads as unfiltered and offers nothing to clear', () => {
    renderFilter()
    expect(trigger()).toHaveTextContent('Any time')
    expect(screen.queryByLabelText('Clear time filter')).toBeNull()
  })

  it('describes the current filter on the trigger', () => {
    renderFilter({
      kind: 'absolute',
      start: localWallClock(2026, 3, 14, 9, 0).toISOString(),
      end: localWallClock(2026, 3, 14, 17, 30).toISOString(),
    })
    expect(trigger()).toHaveTextContent('Mar 14, 09:00 – 17:30')
  })

  it('opens on the trigger and offers the week ending today', () => {
    renderFilter()
    expect(isOpen()).toBe(false)
    open()
    expect(isOpen()).toBe(true)
    expect(day('Mar 8')).toBeInTheDocument()
    expect(day('Mar 13')).toBeInTheDocument()
    expect(day('Today')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Mar 14' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Mar 7' })).toBeNull()
  })

  it('closes on Escape and on a click outside, but not on one inside', () => {
    renderFilter()
    open()
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(isOpen()).toBe(false)

    open()
    fireEvent.mouseDown(day('Mar 13'))
    expect(isOpen()).toBe(true)
    fireEvent.mouseDown(document.body)
    expect(isOpen()).toBe(false)
  })

  it('clears the filter and closes', () => {
    const onChange = renderFilter({ kind: 'relative', minutes: 15 })
    open()
    fireEvent.click(screen.getByLabelText('Clear time filter'))
    expect(onChange).toHaveBeenLastCalledWith(null)
    expect(isOpen()).toBe(false)
    expect(trigger()).toHaveTextContent('Any time')
  })

  describe('presets', () => {
    it('picks a relative filter, and toggles it off when picked again', () => {
      const onChange = renderFilter()
      open()
      fireEvent.click(preset(15))
      expect(onChange).toHaveBeenLastCalledWith({ kind: 'relative', minutes: 15 })
      expect(preset(15)).toHaveAttribute('aria-pressed', 'true')

      fireEvent.click(preset(15))
      expect(onChange).toHaveBeenLastCalledWith(null)
      expect(preset(15)).toHaveAttribute('aria-pressed', 'false')
    })

    it('switches between presets rather than clearing', () => {
      const onChange = renderFilter({ kind: 'relative', minutes: 15 })
      open()
      fireEvent.click(preset(5))
      expect(onChange).toHaveBeenLastCalledWith({ kind: 'relative', minutes: 5 })
    })

    it('shows the range a relative filter currently resolves to', () => {
      renderFilter({ kind: 'relative', minutes: 15 })
      open()
      expect(from()).toHaveValue('11:45')
      expect(until()).toHaveValue('')
      expect(day('Today')).toHaveAttribute('aria-pressed', 'true')
      expect(day('Mar 13')).toHaveAttribute('aria-pressed', 'false')
    })
  })

  describe('the day strip', () => {
    it('picks a whole day', () => {
      const onChange = renderFilter()
      open()
      fireEvent.click(day('Mar 13'))
      expect(onChange).toHaveBeenLastCalledWith(wholeDay(13))
      expect(from()).toHaveValue('00:00')
      expect(until()).toHaveValue('23:59')
      expect(highlightedDays()).toEqual(['Mar 13'])
    })

    it('extends the selection back to an earlier day', () => {
      const onChange = renderFilter()
      open()
      fireEvent.click(day('Mar 13'))
      fireEvent.click(day('Mar 11'))
      expect(onChange).toHaveBeenLastCalledWith({
        kind: 'absolute',
        start: wholeDay(11).start,
        end: wholeDay(13).end,
      })
      expect(highlightedDays()).toEqual(['Mar 11', 'Mar 12', 'Mar 13'])
    })

    it('extends the selection forward to a later day', () => {
      const onChange = renderFilter()
      open()
      fireEvent.click(day('Mar 11'))
      fireEvent.click(day('Mar 13'))
      expect(onChange).toHaveBeenLastCalledWith({
        kind: 'absolute',
        start: wholeDay(11).start,
        end: wholeDay(13).end,
      })
    })

    it('clears when the day of a whole-day selection is picked again', () => {
      const onChange = renderFilter()
      open()
      fireEvent.click(day('Mar 13'))
      fireEvent.click(day('Mar 13'))
      expect(onChange).toHaveBeenLastCalledWith(null)
      expect(highlightedDays()).toEqual([])
    })

    it('widens a part-day selection to the whole day before clearing it', () => {
      const onChange = renderFilter(wholeDay(13))
      open()
      fireEvent.change(from(), { target: { value: '09:00' } })
      fireEvent.click(day('Mar 13'))
      expect(onChange).toHaveBeenLastCalledWith(wholeDay(13))
      fireEvent.click(day('Mar 13'))
      expect(onChange).toHaveBeenLastCalledWith(null)
    })

    it('carries the times over to the next day picked', () => {
      const onChange = renderFilter(wholeDay(13))
      open()
      fireEvent.change(from(), { target: { value: '09:00' } })
      fireEvent.change(until(), { target: { value: '17:00' } })
      fireEvent.click(day('Mar 11'))
      expect(onChange).toHaveBeenLastCalledWith({
        kind: 'absolute',
        start: localWallClock(2026, 3, 11, 9, 0).toISOString(),
        end: localWallClock(2026, 3, 13, 17, 0).toISOString(),
      })
    })

    it('does not carry over the times a relative filter happens to resolve to', () => {
      const onChange = renderFilter({ kind: 'relative', minutes: 15 })
      open()
      fireEvent.click(day('Mar 13'))
      expect(onChange).toHaveBeenLastCalledWith(wholeDay(13))
    })
  })

  describe('the time fields', () => {
    it('moves a bound within the selected day', () => {
      const onChange = renderFilter(wholeDay(13))
      open()
      fireEvent.change(from(), { target: { value: '09:00' } })
      expect(onChange).toHaveBeenLastCalledWith({
        kind: 'absolute',
        start: localWallClock(2026, 3, 13, 9, 0).toISOString(),
        end: wholeDay(13).end,
      })

      fireEvent.change(until(), { target: { value: '17:00' } })
      expect(onChange).toHaveBeenLastCalledWith({
        kind: 'absolute',
        start: localWallClock(2026, 3, 13, 9, 0).toISOString(),
        end: localWallClock(2026, 3, 13, 17, 0).toISOString(),
      })
    })

    it('ignores an incomplete value', () => {
      const onChange = renderFilter(wholeDay(13))
      open()
      fireEvent.change(from(), { target: { value: '' } })
      expect(onChange).not.toHaveBeenCalled()
    })

    it('freezes a relative filter into the range it resolved to', () => {
      const onChange = renderFilter({ kind: 'relative', minutes: 15 })
      open()
      fireEvent.change(from(), { target: { value: '09:00' } })
      expect(onChange).toHaveBeenLastCalledWith({
        kind: 'absolute',
        start: localWallClock(2026, 3, 14, 9, 0).toISOString(),
        end: undefined,
      })
    })

    it('allows an inverted range but highlights nothing', () => {
      renderFilter(wholeDay(13))
      open()
      fireEvent.change(from(), { target: { value: '17:00' } })
      fireEvent.change(until(), { target: { value: '09:00' } })
      expect(from()).toHaveValue('17:00')
      expect(until()).toHaveValue('09:00')
      expect(highlightedDays()).toEqual([])
    })
  })
})
