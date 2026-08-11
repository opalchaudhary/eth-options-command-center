export function formatNumber(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "NA";
  return value.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

export function formatPrice(value: number | null | undefined) {
  return value === null || value === undefined ? "NA" : `$${formatNumber(value, 2)}`;
}

export function formatGreek(value: number | null | undefined) {
  return formatNumber(value, 4);
}

export function formatPct(value: number | null | undefined) {
  return value === null || value === undefined ? "NA" : `${formatNumber(value, 2)}%`;
}

export function formatDate(value: string | null | undefined) {
  if (!value) return "NA";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

export function pnlColor(value: number | null | undefined, positive: string, negative: string, neutral: string) {
  if (value === null || value === undefined || value === 0) return neutral;
  return value > 0 ? positive : negative;
}
