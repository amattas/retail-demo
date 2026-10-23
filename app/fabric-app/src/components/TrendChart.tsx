import { useId, useState } from 'react';

export interface ChartPoint {
  label: string;
  previousLabel: string;
  current: number;
  previous: number;
}

const WIDTH = 760;
const HEIGHT = 240;
const PAD = { top: 16, right: 16, bottom: 28, left: 52 };

function niceBounds(values: number[]): [number, number] {
  let min = Math.min(...values);
  let max = Math.max(...values);
  if (min === max) { min -= 1; max += 1; }
  const pad = (max - min) * 0.12;
  return [Math.max(0, min - pad), max + pad];
}

/** Current period (solid) against the previous period (dashed), like a TY/LY trend. */
export function TrendChart({ points, format, title }: {
  points: ChartPoint[]; format: (value: number) => string; title: string;
}) {
  const gradient = useId().replaceAll(':', '');
  const [hover, setHover] = useState<number | null>(null);
  if (!points.length) return null;

  const [min, max] = niceBounds(points.flatMap((p) => [p.current, p.previous]));
  const innerW = WIDTH - PAD.left - PAD.right;
  const innerH = HEIGHT - PAD.top - PAD.bottom;
  const x = (i: number) => PAD.left + (points.length === 1 ? innerW / 2 : (i / (points.length - 1)) * innerW);
  const y = (v: number) => PAD.top + innerH - ((v - min) / (max - min)) * innerH;
  const line = (key: 'current' | 'previous') =>
    points.map((p, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(p[key]).toFixed(1)}`).join(' ');
  const area = `${line('current')} L${x(points.length - 1)},${PAD.top + innerH} L${x(0)},${PAD.top + innerH} Z`;
  const ticks = Array.from({ length: 4 }, (_, i) => min + ((max - min) * i) / 3);
  const labelEvery = Math.max(1, Math.ceil(points.length / 7));
  const active = hover === null ? null : points[hover];

  const onMove = (event: React.MouseEvent<SVGRectElement>) => {
    const box = event.currentTarget.getBoundingClientRect();
    const ratio = (event.clientX - box.left) / box.width;
    setHover(Math.min(points.length - 1, Math.max(0, Math.round(ratio * (points.length - 1)))));
  };

  return (
    <div className="chart-box">
      <svg className="chart" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={title}>
        <defs>
          <linearGradient id={gradient} x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="var(--cp-accent)" stopOpacity="0.22" />
            <stop offset="100%" stopColor="var(--cp-accent)" stopOpacity="0" />
          </linearGradient>
        </defs>
        {ticks.map((tick) => (
          <g key={tick}>
            <line className="grid-line" x1={PAD.left} x2={WIDTH - PAD.right} y1={y(tick)} y2={y(tick)} />
            <text className="axis-label" x={PAD.left - 8} y={y(tick) + 4} textAnchor="end">{format(tick)}</text>
          </g>
        ))}
        {points.map((p, i) => (i % labelEvery === 0 || i === points.length - 1) && (
          <text key={p.label} className="axis-label" x={x(i)} y={HEIGHT - 8} textAnchor="middle">{p.label}</text>
        ))}
        <path d={area} fill={`url(#${gradient})`} />
        <path className="series-previous" d={line('previous')} />
        <path className="series-current" d={line('current')} />
        {active && hover !== null && <>
          <line className="hover-line" x1={x(hover)} x2={x(hover)} y1={PAD.top} y2={PAD.top + innerH} />
          <circle className="dot" cx={x(hover)} cy={y(active.current)} r="4.5" />
        </>}
        <rect
          x={PAD.left} y={PAD.top} width={innerW} height={innerH} fill="transparent"
          onMouseMove={onMove} onMouseLeave={() => setHover(null)}
        />
      </svg>
      {active && hover !== null && (
        <div className="chart-tooltip" style={{ left: `${(x(hover) / WIDTH) * 100}%`, top: `${(y(active.current) / HEIGHT) * 100}%` }}>
          <b>{active.label}: {format(active.current)}</b>
          Prior ({active.previousLabel}): {format(active.previous)}
        </div>
      )}
    </div>
  );
}
