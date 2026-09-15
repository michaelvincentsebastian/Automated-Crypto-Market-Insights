import React from "react";

export function Card({
  className = "",
  children,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={`bg-[#0b120e]/95 border border-[#1a2e22]/80 rounded-xl shadow-lg shadow-black/40 backdrop-blur-md relative overflow-hidden transition-all duration-200 hover:border-[#00ff88]/30 ${className}`}
      {...props}
    >
      {/* Subtle top edge phosphor highlight */}
      <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-[#00ff88]/20 to-transparent pointer-events-none" />
      {children}
    </div>
  );
}

export function CardHeader({
  className = "",
  children,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={`p-5 pb-2 ${className}`} {...props}>
      {children}
    </div>
  );
}

export function CardTitle({
  className = "",
  children,
  ...props
}: React.HTMLAttributes<HTMLHeadingElement>) {
  return (
    <h3
      className={`text-xs font-semibold uppercase tracking-wider text-[#7e9c8b] flex items-center justify-between font-display ${className}`}
      {...props}
    >
      {children}
    </h3>
  );
}

export function CardContent({
  className = "",
  children,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={`p-5 pt-3 ${className}`} {...props}>
      {children}
    </div>
  );
}
