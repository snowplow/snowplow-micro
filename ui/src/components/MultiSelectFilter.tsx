import { useState, useMemo } from 'react'
import {
  Combobox,
  ComboboxChips,
  ComboboxChipsInput,
  ComboboxChip,
  ComboboxContent,
  ComboboxItem,
  ComboboxList,
  ComboboxValue,
  useComboboxAnchor,
} from '@/components/ui/combobox'

type MultiSelectFilterProps = {
  options: string[]
  selectedValues: string[]
  onChange: (values: string[]) => void
  placeholder: string
}

export function MultiSelectFilter({
  options,
  selectedValues,
  onChange,
  placeholder,
}: MultiSelectFilterProps) {
  const [inputValue, setInputValue] = useState('')
  const anchorRef = useComboboxAnchor()

  // Filter out nulls, include selected custom values and the current typed value
  const items = useMemo(() => {
    const validOptions = options.filter((o) => o != null && o !== '')
    const allValues = new Set([...validOptions, ...selectedValues])
    const trimmed = inputValue.trim()
    if (trimmed) allValues.add(trimmed)
    return [...allValues].sort()
  }, [options, selectedValues, inputValue])

  return (
    <Combobox
      items={items}
      multiple
      value={selectedValues}
      onValueChange={(newValues) => {
        onChange(newValues)
        setInputValue('')
      }}
    >
      <ComboboxChips ref={anchorRef}>
        <ComboboxValue>
          {selectedValues.map((item) => (
            <ComboboxChip key={item}>{item}</ComboboxChip>
          ))}
        </ComboboxValue>
        <ComboboxChipsInput
          placeholder={selectedValues.length === 0 ? placeholder : ''}
          onInput={(e: React.FormEvent<HTMLInputElement>) =>
            setInputValue((e.target as HTMLInputElement).value)
          }
          onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => {
            if (e.key === 'Enter') {
              const val = (e.target as HTMLInputElement).value.trim()
              if (val && !selectedValues.includes(val)) {
                e.preventDefault()
                e.stopPropagation()
                onChange([...selectedValues, val])
                setInputValue('')
              }
            }
          }}
        />
      </ComboboxChips>
      {items.length > 0 && (
        <ComboboxContent anchor={anchorRef}>
          <ComboboxList>
            {(item) => (
              <ComboboxItem key={item} value={item}>
                {item}
              </ComboboxItem>
            )}
          </ComboboxList>
        </ComboboxContent>
      )}
    </Combobox>
  )
}
