import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { InfoTooltip } from "@/components/ui/tooltip";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { SentimentDialIcon } from "@/components/ui/cyber-icons";
import { GLOSSARY } from "@/lib/glossary";
import type { FearGreedData } from "@/types/crypto";

interface FearGreedGaugeProps {
  data?: FearGreedData;
  isLoading?: boolean;
}

export function FearGreedGauge({ data, isLoading }: FearGreedGaugeProps) {
  if (isLoading || !data) {
    return (
      <Card className="h-full">
        <CardHeader className="p-5 pb-2">
          <Skeleton className="h-5 w-40 bg-[#0e1713]" />
        </CardHeader>
        <CardContent className="p-5 flex flex-col items-center justify-center min-h-[220px]">
          <Skeleton className="h-28 w-48 rounded-full mb-3 bg-[#0e1713]" />
          <Skeleton className="h-6 w-24 mb-1 bg-[#0e1713]" />
          <Skeleton className="h-4 w-32 bg-[#0e1713]" />
        </CardContent>
      </Card>
    );
  }

  const score = Math.max(0, Math.min(100, data.value));
  const rotation = -90 + (score / 100) * 180;

  let colorClass = "text-amber-400";
  let badgeVariant: "danger" | "warning" | "success" | "secondary" = "warning";

  if (score < 25) {
    colorClass = "text-[#ff3366]";
    badgeVariant = "danger";
  } else if (score < 45) {
    colorClass = "text-orange-400";
    badgeVariant = "danger";
  } else if (score <= 55) {
    colorClass = "text-amber-400";
    badgeVariant = "warning";
  } else if (score <= 75) {
    colorClass = "text-[#00ff88]";
    badgeVariant = "success";
  } else {
    colorClass = "text-[#00ff88] drop-shadow-[0_0_8px_rgba(0,255,136,0.5)]";
    badgeVariant = "success";
  }

  return (
    <Card className="h-full flex flex-col justify-between hover:border-[#00ff88]/30 transition-all duration-200">
      <CardHeader className="p-5 pb-1">
        <CardTitle className="text-xs text-[#7e9c8b] font-semibold font-display tracking-wider flex items-center justify-between">
          <span className="flex items-center gap-2">
            <span className="p-1 rounded bg-[#0e1713] border border-[#1a2e22] text-[#00ff88]">
              <SentimentDialIcon size={16} />
            </span>
            <span>FEAR & GREED SENTIMENT</span>
          </span>
          <InfoTooltip
            title={GLOSSARY.fearGreedIndex.title}
            content={GLOSSARY.fearGreedIndex.shortDesc}
            detail={GLOSSARY.fearGreedIndex.detail}
            align="right"
          />
        </CardTitle>
      </CardHeader>

      <CardContent className="p-5 pt-2 flex flex-col items-center justify-center">
        {/* SVG Cyber Gauge with prominent instrument tuas (needle) */}
        <div className="relative w-64 h-34 flex items-end justify-center">
          <svg viewBox="0 0 200 118" className="w-full h-auto drop-shadow-[0_4px_12px_rgba(0,0,0,0.5)]">
            <defs>
              <linearGradient id="cyberGaugeGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="#ff3366" />
                <stop offset="25%" stopColor="#f97316" />
                <stop offset="50%" stopColor="#eab308" />
                <stop offset="75%" stopColor="#10b981" />
                <stop offset="100%" stopColor="#00ff88" />
              </linearGradient>
            </defs>

            {/* Background Track Arc */}
            <path
              d="M 20 100 A 80 80 0 0 1 180 100"
              fill="none"
              stroke="#0f1914"
              strokeWidth="14"
              strokeLinecap="round"
            />

            {/* Colored Gradient Arc */}
            <path
              d="M 20 100 A 80 80 0 0 1 180 100"
              fill="none"
              stroke="url(#cyberGaugeGradient)"
              strokeWidth="14"
              strokeLinecap="round"
              opacity="0.95"
            />

            {/* Scale Marker Ticks */}
            {/* 0 (Fear) */}
            <line x1="20" y1="100" x2="28" y2="100" stroke="#050807" strokeWidth="2" />
            {/* 25 */}
            <line x1="43.4" y1="43.4" x2="49.1" y2="49.1" stroke="#050807" strokeWidth="2" />
            {/* 50 (Neutral) */}
            <line x1="100" y1="20" x2="100" y2="28" stroke="#050807" strokeWidth="2" />
            {/* 75 */}
            <line x1="156.6" y1="43.4" x2="150.9" y2="49.1" stroke="#050807" strokeWidth="2" />
            {/* 100 (Greed) */}
            <line x1="180" y1="100" x2="172" y2="100" stroke="#050807" strokeWidth="2" />

            {/* Needle Lever (Tuas Indikator) */}
            <g
              style={{
                transform: `rotate(${rotation}deg)`,
                transformOrigin: "100px 100px",
                transition: "transform 1s cubic-bezier(0.34, 1.56, 0.64, 1)",
              }}
            >
              {/* Needle Arm - Tapered Blade in High-Contrast Ice White */}
              <polygon
                points="96.5,100 99,18 100,13 101,18 103.5,100 100,105"
                fill="#ffffff"
                stroke="#00ff88"
                strokeWidth="1.25"
              />
              {/* Inner Laser Spine for Cyber Glow */}
              <line
                x1="100"
                y1="96"
                x2="100"
                y2="20"
                stroke="#00ff88"
                strokeWidth="1.5"
                strokeLinecap="round"
              />
              {/* Pointer Tip Node penetrating through the arc */}
              <circle cx="100" cy="15" r="2.8" fill="#00ff88" stroke="#ffffff" strokeWidth="0.8" />
              {/* Counter-weight tail */}
              <circle cx="100" cy="103" r="2" fill="#00ff88" />
            </g>

            {/* Needle Pivot Hub (rendered on top of lever) */}
            <circle cx="100" cy="100" r="10" fill="#050807" stroke="#00ff88" strokeWidth="2.5" />
            <circle cx="100" cy="100" r="5" fill="#0b1712" stroke="#00ff88" strokeWidth="1" />
            <circle cx="100" cy="100" r="2.5" fill="#ffffff" />
          </svg>

          {/* Scale Labels */}
          <span className="absolute left-2 bottom-0 text-[10px] font-mono font-bold text-[#ff3366]">0</span>
          <span className="absolute left-1/2 -translate-x-1/2 top-0 text-[10px] font-mono text-[#eab308]">50</span>
          <span className="absolute right-2 bottom-0 text-[10px] font-mono font-bold text-[#00ff88]">100</span>
        </div>

        {/* Score and Sentiment Label */}
        <div className="mt-3 text-center">
          <div className="flex items-center justify-center gap-2">
            <span className={`text-3xl font-extrabold font-mono tabular-nums tracking-tight ${colorClass}`}>
              {score}
            </span>
            <Badge variant={badgeVariant} className="text-xs px-2.5 py-0.5 font-bold">
              {data.classification}
            </Badge>
          </div>
          <p className="text-[11px] text-[#7e9c8b] mt-1 max-w-[240px] leading-relaxed">
            {score > 60
              ? "Sentimen pasar optimis (Greed). Minat beli dan likuiditas tinggi."
              : score < 40
              ? "Sentimen pasar dilanda ketakutan (Fear). Tekanan jual dominan."
              : "Sentimen pasar saat ini berada dalam posisi netral / sideways."}
          </p>
        </div>
      </CardContent>
    </Card>
  );
}
