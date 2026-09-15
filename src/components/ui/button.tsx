import React from "react";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "outline" | "ghost" | "danger" | "cyan";
  size?: "sm" | "md" | "lg" | "icon";
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className = "", variant = "primary", size = "md", children, disabled, ...props }, ref) => {
    const base = "inline-flex items-center justify-center font-display font-medium rounded transition-all duration-150 focus:outline-none disabled:opacity-50 disabled:pointer-events-none active:scale-[0.98]";

    const variants = {
      primary: "bg-[#00ff88] hover:bg-[#00e57a] text-[#050807] font-bold shadow-[0_0_16px_rgba(0,255,136,0.35)] hover:shadow-[0_0_24px_rgba(0,255,136,0.5)]",
      secondary: "bg-[#0e1713] hover:bg-[#13221b] text-[#b9cbb9] hover:text-[#f1ffef] border border-[#00ff88]/20 hover:border-[#00ff88]/50",
      outline: "border border-[#1a2e22] text-[#7e9c8b] hover:bg-[#0b120e] hover:text-[#00ff88] hover:border-[#00ff88]/30",
      ghost: "text-[#7e9c8b] hover:text-[#00ff88] hover:bg-[#0b120e]/60",
      danger: "bg-[#ff3366] hover:bg-[#e02657] text-[#050807] font-bold shadow-[0_0_16px_rgba(255,51,102,0.35)]",
      cyan: "bg-[#00e5ff] hover:bg-[#00cbe3] text-[#050807] font-bold shadow-[0_0_16px_rgba(0,229,255,0.35)]",
    };

    const sizes = {
      sm: "text-xs px-2.5 py-1.5 gap-1.5",
      md: "text-sm px-3.5 py-2 gap-2",
      lg: "text-base px-5 py-2.5 gap-2.5",
      icon: "h-9 w-9 p-0",
    };

    return (
      <button
        ref={ref}
        disabled={disabled}
        className={`${base} ${variants[variant]} ${sizes[size]} ${className}`}
        {...props}
      >
        {children}
      </button>
    );
  }
);

Button.displayName = "Button";
