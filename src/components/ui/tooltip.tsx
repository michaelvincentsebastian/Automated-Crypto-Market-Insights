import React, { useState } from "react";
import { Info } from "lucide-react";

interface InfoTooltipProps {
  title?: string;
  content: string;
  detail?: string;
  children?: React.ReactNode;
  align?: "left" | "center" | "right";
}

export function InfoTooltip({
  title,
  content,
  detail,
  children,
  align = "center",
}: InfoTooltipProps) {
  const [isOpen, setIsOpen] = useState(false);

  const alignmentClass =
    align === "left"
      ? "left-0"
      : align === "right"
      ? "right-0"
      : "left-1/2 -translate-x-1/2";

  return (
    <div
      className="relative inline-flex items-center group cursor-pointer"
      onMouseEnter={() => setIsOpen(true)}
      onMouseLeave={() => setIsOpen(false)}
      onClick={(e) => {
        e.stopPropagation();
        setIsOpen(!isOpen);
      }}
    >
      {children || (
        <Info className="w-3.5 h-3.5 ml-1.5 text-slate-500 hover:text-indigo-400 transition-colors" />
      )}

      {isOpen && (
        <div
          className={`absolute bottom-full mb-2 z-50 w-64 p-3 bg-slate-900/95 border border-slate-700/80 rounded-lg shadow-xl backdrop-blur-md text-xs text-slate-200 pointer-events-none transition-all duration-150 animate-in fade-in-50 zoom-in-95 ${alignmentClass}`}
        >
          {title && (
            <div className="font-semibold text-indigo-300 pb-1 mb-1 border-b border-slate-800 flex items-center justify-between">
              <span>{title}</span>
            </div>
          )}
          <p className="leading-relaxed text-slate-300">{content}</p>
          {detail && (
            <p className="mt-1.5 pt-1.5 border-t border-slate-800/80 text-[11px] text-slate-400 italic leading-snug">
              {detail}
            </p>
          )}
          <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-slate-800" />
        </div>
      )}
    </div>
  );
}
