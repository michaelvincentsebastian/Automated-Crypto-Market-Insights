import React from "react";

interface IconProps extends React.SVGProps<SVGSVGElement> {
  size?: number;
  className?: string;
}

// 1. CryptSight Official Dashboard Logo: Cyber Optical Sight & Crypto Aperture
export function CryptSightLogo({ size = 26, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 28 28"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      {...props}
    >
      {/* Outer Hex Shield Bezel */}
      <polygon
        points="14 2 24.5 7.5 24.5 20.5 14 26 3.5 20.5 3.5 7.5"
        stroke="#00ff88"
        strokeWidth="1.75"
        strokeLinejoin="round"
        fill="#07110c"
      />
      {/* Sight Reticle Outer Ring */}
      <circle cx="14" cy="14" r="7.5" stroke="#00ff88" strokeWidth="1.25" strokeDasharray="3 2" opacity="0.8" />
      {/* Optical Sight Crosshairs */}
      <line x1="14" y1="4" x2="14" y2="8" stroke="#00ff88" strokeWidth="1.5" strokeLinecap="round" />
      <line x1="14" y1="20" x2="14" y2="24" stroke="#00ff88" strokeWidth="1.5" strokeLinecap="round" />
      <line x1="4" y1="14" x2="8" y2="14" stroke="#00ff88" strokeWidth="1.5" strokeLinecap="round" />
      <line x1="20" y1="14" x2="24" y2="14" stroke="#00ff88" strokeWidth="1.5" strokeLinecap="round" />
      {/* Core Crypto Diamond Sight */}
      <polygon points="14 9.5 18.5 14 14 18.5 9.5 14" fill="#00ff88" fillOpacity="0.2" stroke="#00ff88" strokeWidth="1.5" />
      {/* Intense White/Neon Core Center */}
      <circle cx="14" cy="14" r="2.2" fill="#ffffff" />
      <circle cx="14" cy="14" r="1" fill="#00ff88" />
    </svg>
  );
}

// 1b. Legacy CyberLogoIcon alias for backwards compatibility
export function CyberLogoIcon({ size = 24, className = "", ...props }: IconProps) {
  return <CryptSightLogo size={size} className={className} {...props} />;
}

// 2. Live Beacon: Pulsing radar node
export function LiveBeaconIcon({ className = "" }: { className?: string }) {
  return (
    <span className={`relative flex h-2.5 w-2.5 items-center justify-center ${className}`}>
      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-[#00ff88] opacity-75" />
      <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-[#00ff88]" />
    </span>
  );
}

// 3. Market Cap: Telemetry digital vault
export function MarketCapIcon({ size = 18, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <rect x="2" y="7" width="20" height="14" rx="2" stroke="#00ff88" />
      <path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16" stroke="#7e9c8b" />
      <circle cx="12" cy="14" r="2" fill="#00ff88" />
    </svg>
  );
}

// 4. 24h Volume: Dual laser signal bars
export function VolumeSignalIcon({ size = 18, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <path d="M12 20V10" stroke="#00ff88" strokeWidth="2.5" strokeLinecap="round" />
      <path d="M18 20V4" stroke="#00ff88" strokeWidth="2.5" strokeLinecap="round" opacity="0.85" />
      <path d="M6 20v-4" stroke="#7e9c8b" strokeWidth="2.5" strokeLinecap="round" />
    </svg>
  );
}

// 5. BTC Dominance: Radar target circle with crosshair
export function BtcDominanceIcon({ size = 18, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <circle cx="12" cy="12" r="9" stroke="#7e9c8b" strokeWidth="1.5" />
      <circle cx="12" cy="12" r="5" stroke="#00ff88" strokeWidth="1.75" strokeDasharray="3 3" />
      <line x1="12" y1="3" x2="12" y2="7" stroke="#00ff88" />
      <line x1="12" y1="17" x2="12" y2="21" stroke="#00ff88" />
      <line x1="3" y1="12" x2="7" y2="12" stroke="#00ff88" />
      <line x1="17" y1="12" x2="21" y2="12" stroke="#00ff88" />
      <circle cx="12" cy="12" r="1.5" fill="#00ff88" />
    </svg>
  );
}

// 6. ETH Dominance: Interconnected dual smart contract diamond
export function EthDominanceIcon({ size = 18, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <polygon points="12 2 19 12 12 16 5 12 12 2" stroke="#00e5ff" fill="rgba(0, 229, 255, 0.08)" />
      <polygon points="12 16 19 12 12 22 5 12 12 16" stroke="#00ff88" fill="rgba(0, 255, 136, 0.08)" />
    </svg>
  );
}

// 7. Active Cryptos: Digital network mesh node
export function ActiveClusterIcon({ size = 18, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <circle cx="6" cy="6" r="2.5" stroke="#00ff88" fill="#0b120e" />
      <circle cx="18" cy="6" r="2.5" stroke="#7e9c8b" fill="#0b120e" />
      <circle cx="12" cy="18" r="2.5" stroke="#00ff88" fill="#0b120e" />
      <line x1="8.5" y1="6" x2="15.5" y2="6" stroke="#1a2e22" />
      <line x1="7.5" y1="8" x2="10.5" y2="16" stroke="#1a2e22" />
      <line x1="16.5" y1="8" x2="13.5" y2="16" stroke="#1a2e22" />
    </svg>
  );
}

// 8. Fear & Greed: Cyber sensor compass
export function SentimentDialIcon({ size = 18, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <path d="M4 14a8 8 0 1 1 16 0" stroke="#7e9c8b" strokeWidth="1.5" />
      <path d="M12 14l3-6" stroke="#00ff88" strokeWidth="2" strokeLinecap="round" />
      <circle cx="12" cy="14" r="2.5" fill="#00ff88" />
    </svg>
  );
}

// 9. Neon Gainers Arrow
export function NeonUpIcon({ size = 16, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="#00ff88"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <line x1="7" y1="17" x2="17" y2="7" />
      <polyline points="7 7 17 7 17 17" />
    </svg>
  );
}

// 10. Neon Losers Arrow
export function NeonDownIcon({ size = 16, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="#ff3366"
      strokeWidth="2.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <line x1="7" y1="7" x2="17" y2="17" />
      <polyline points="17 7 17 17 7 17" />
    </svg>
  );
}

// 11. Zero-Copy Shield
export function ZeroCopyShieldIcon({ size = 18, className = "", ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      {...props}
    >
      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" stroke="#00ff88" fill="rgba(0, 255, 136, 0.08)" />
      <polyline points="9 12 11 14 15 10" stroke="#00ff88" strokeWidth="2" />
    </svg>
  );
}
