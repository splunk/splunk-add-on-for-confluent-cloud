import React, { useState } from "react";
import { useEffect } from "react";
import Button from "@splunk/react-ui/Button";
import TextArea from "@splunk/react-ui/TextArea";
import ArrowsCircularDouble from "@splunk/react-icons/ArrowsCircularDouble";
import * as config from "@splunk/splunk-utils/config";
import { createRESTURL } from "@splunk/splunk-utils/url";
import {
  handleResponse,
  defaultFetchInit,
} from "@splunk/splunk-utils/fetch";

const PLACEHOLDER =
    "Click 'Discover Metrics' to start the discovery process. Results will appear here…";
const MIN_ROWS = 10;
const calcRows = (txt: string) =>
    Math.max(txt.split("\n").length + 1, MIN_ROWS);

interface MetricsDiscoveryProps {
  onDiscoveryStart: () => void;
  onDiscoveryComplete: (success: boolean) => void;
}

interface DiscoveryResponse {
  inserted: number;
  updated: number;
  unchanged: number;
  total_ga: number;
  deprecated: number;
  last_run: string;
}

const MetricsDiscovery: React.FC<MetricsDiscoveryProps> = ({ 
  onDiscoveryStart, 
  onDiscoveryComplete 
}) => {
  const [textValue, setTextValue] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    // Fetch latest discovery status on mount
    const fetchStatus = async () => {
      try {
        const url = createRESTURL("confluent_metrics/discovery_status", {
          app: config.app,
          sharing: "app",
        });
        const response = await fetch(url, {
          ...defaultFetchInit,
          method: "GET",
          headers: {
            "X-Splunk-Form-Key": config.CSRFToken,
            "X-Requested-With": "XMLHttpRequest",
          },
        });
        
        if (response.ok) {
          const result = await response.json();
          console.log("Discovery status response:", result);
          if (result?.last_run) {
                setTextValue(formatDiscoveryResults(result as DiscoveryResponse));
                return;
            }
        }
      } catch (err) {
        // Ignore errors, fallback to default text
      }
    };
    fetchStatus();
  }, []);

  const formatDiscoveryResults = (data: DiscoveryResponse): string => {
    return `Metrics Discovery Results:
─────────────────────────────
Metrics added: ${data.inserted}
Metrics updated: ${data.updated}
Metrics unchanged: ${data.unchanged}
Generally available metrics: ${data.total_ga}
Deprecated metrics: ${data.deprecated}
Last run: ${data.last_run}
─────────────────────────────

Summary:
• Total metrics processed: ${data.inserted + data.updated + data.unchanged}
• Changes made: ${data.inserted + data.updated}
• Discovery completed successfully!`;
  };

  const handleClick = async () => {
    setLoading(true);
    
    // Clear previous results
    setTextValue("Starting metrics discovery...");
    
    // Notify parent component that discovery has started
    onDiscoveryStart();
    
    try {
      // Create REST URL for metrics refresh endpoint
      const url = createRESTURL("confluent_metrics/refresh", {
        app: config.app,
        sharing: "app",
      });

      // Prepare request configuration
      const init = {
        ...defaultFetchInit,
        method: "POST",
        headers: {
          "X-Splunk-Form-Key": config.CSRFToken,
          "X-Requested-With": "XMLHttpRequest",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}), // Empty body - uses default account
      };

      // Make API call to metrics_refresh_handler.py
      const response = await fetch(url, init);
      const data: DiscoveryResponse = await handleResponse(200)(response);
      
      // Format the response data for display
      const formattedOutput = formatDiscoveryResults(data);
      setTextValue(formattedOutput);
      
      // Notify parent component of successful completion
      onDiscoveryComplete(true);
      
    } catch (err) {
      // Handle errors gracefully
      let errorMessage = "Failed to discover metrics.";
      
      if (typeof err === "string") {
        errorMessage = err;
      } else if (err instanceof Error) {
        errorMessage = err.message;
      } else if (err && typeof err === "object" && "message" in err) {
        errorMessage = String(err.message);
      }
      
      const errorOutput = `Discovery Failed:
─────────────────────────────
Error: ${errorMessage}
Time: ${new Date().toLocaleString()}
─────────────────────────────

Please check:
• Confluent Cloud API credentials are configured
• Network connectivity to Confluent Cloud
• Splunk logs for detailed error information

You can try discovery again once issues are resolved.`;
      
      setTextValue(errorOutput);
      
      // Notify parent component of failed completion
      onDiscoveryComplete(false);
      
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 600, marginBottom: '24px' }}>
      {/* Discovery Button */}
      <Button
        appearance="primary"
        onClick={handleClick}
        disabled={loading}
        style={{ 
          display: "inline-flex", 
          alignItems: "center",
          minWidth: "180px" // Prevent button resize during loading
        }}
      >
        {/* Refresh Icon */}
        <span
          style={{
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            width: 16,
            height: 16,
            marginRight: 8,
            color: "currentColor",
            animation: loading ? "spin 2s linear infinite" : "none",
          }}
        >
          <ArrowsCircularDouble />
        </span>
        {loading ? "Discovering..." : "Discover Metrics"}
      </Button>

      {/* Results TextArea */}
      <TextArea
          value={textValue || PLACEHOLDER}
          disabled
          onChange={() => {}}
          rowsMin={calcRows(textValue || PLACEHOLDER)}
          style={{
            marginTop: 12,
            width: "100%",
            fontFamily: "monospace",
            fontSize: "13px",
            resize: "vertical",
            whiteSpace: "pre-wrap"
          }}
      />
      
      {/* Add CSS for spin animation */}
      <style>
        {`
          @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
          }
        `}
      </style>
    </div>
  );
};

export default MetricsDiscovery;