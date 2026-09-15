import React from "react";

interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: "default" | "secondary" | "outline" | "success" | "danger" | "warning" | "cyan";
}

export function Badge({
  className = "",
  variant = "default",
  children,
  ...props
}: BadgeProps) {
  const variantStyles = {
    default: "bg-[#00ff88]/10 text-[#00ff88] border-[#00ff88]/25 shadow-[0_0_8px_rgba(0,255,136,0.15)]",
    secondary: "bg-[#0e1713] text-[#b9cbb9] border-[#1a2e22]",
    outline: "bg-transparent text-[#7e9c8b] border-[#1a2e22]",
    success: "bg-[#00ff88]/10 text-[#00ff88] border-[#00ff88]/25 shadow-[0_0_8px_rgba(0,255,136,0.15)]",
    danger: "bg-[#ff3366]/10 text-[#ff3366] border-[#ff3366]/25 shadow-[0_0_8px_rgba(255,51,102,0.15)]",
    warning: "bg-amber-400/10 text-amber-400 border-amber-400/25",
    cyan: "bg-[#00e5ff]/10 text-[#00e5ff] border-[#00e5ff]/25 shadow-[0_0_8px_rgba(0,229,255,0.15)]",
  };

  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-[10px] font-mono font-medium tracking-wide uppercase border tabular-nums ${variantStyles[variant]} ${className}`}
      {...props}
    >
      {children}
    </span>
  );
}
