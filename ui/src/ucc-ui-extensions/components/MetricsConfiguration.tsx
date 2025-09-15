import React, { useState, useEffect } from 'react';
import WaitSpinner from '@splunk/react-ui/WaitSpinner';
import Typography from '@splunk/react-ui/Typography';
import Heading from '@splunk/react-ui/Heading';
import { SplunkThemeProvider } from '@splunk/themes';
import styled from 'styled-components';
import MetricsDiscovery from './MetricsDiscovery';
import MetricsTable from './MetricsTable';
import * as config from "@splunk/splunk-utils/config";
import { createRESTURL } from "@splunk/splunk-utils/url";
import {
  handleResponse,
  defaultFetchInit,
} from "@splunk/splunk-utils/fetch";

/* ---------- TypeScript Interfaces (ALIGNED) ---------- */

// Response format for basic list call (names only)
interface MetricsListResponse {
  namespaces?: Record<string, string[]>;
  total_metrics?: number;
  metrics_by_status?: {
    enabled: number;
    disabled: number;
  };
  namespace_summary?: Record<string, number>;
}

/* ---------- Styled Components ---------- */
const ConfigurationContainer = styled.div`
  padding: 20px;
  max-width: 100%;
`;

const LoadingContainer = styled.div`
  text-align: center;
  padding: 40px;
`;

const EmptyStateContainer = styled.div`
  text-align: center;
  padding: 60px 40px;
  background-color: #f8f9fa;
  border: 1px solid #e9ecef;
  border-radius: 8px;
  margin-top: 24px;
`;

const DiscoveryLoadingContainer = styled.div`
  text-align: center;
  padding: 40px;
  background-color: #fff;
  border: 1px solid #e0e0e0;
  border-radius: 8px;
  margin-top: 24px;
`;

const ErrorContainer = styled.div`
  background-color: #f8d7da;
  color: #721c24;
  padding: 16px;
  margin: 16px 0;
  border: 1px solid #f5c6cb;
  border-radius: 4px;
`;

/* ---------- Main Component ---------- */
export default function MetricsConfiguration() {
  console.log("[DEBUG] MetricsConfiguration component is mounting!");
  // Core state management
  const [hasMetrics, setHasMetrics] = useState(false);
  const [isDiscovering, setIsDiscovering] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [initialLoading, setInitialLoading] = useState(true);
  
  // Error handling
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  
  // Metrics count for display
  const [metricsCount, setMetricsCount] = useState(0);

  useEffect(() => {
    console.log("[DEBUG] MetricsConfiguration state changed:");
    console.log("  hasMetrics:", hasMetrics);
    console.log("  isDiscovering:", isDiscovering);
    console.log("  initialLoading:", initialLoading);
    console.log("  metricsCount:", metricsCount);
    console.log("  refreshKey:", refreshKey);
  }, [hasMetrics, isDiscovering, initialLoading, metricsCount, refreshKey]);


  /**
   * Check if metrics exist in the KV store on component mount
   * Uses the lightweight list endpoint (no metric parameter)
   * Aligned with metrics_list_handler.py default behavior
   */
  const checkMetricsExist = async () => {
    try {
      setErrorMessage(null);
      
      console.log("🔍 Checking for existing metrics...");
      
      // Use basic list endpoint (no metric parameter) for lightweight check
      const url = createRESTURL("confluent_metrics/list", {
        app: config.app,
        sharing: "app",
      });
      
      console.log("[DEBUG] Making basic check request to:", url);

      const response = await fetch(url, {
        ...defaultFetchInit,
        method: "GET",
        headers: {
          "X-Splunk-Form-Key": config.CSRFToken,
          "X-Requested-With": "XMLHttpRequest",
        },
      });
      
      const result: MetricsListResponse = await handleResponse(200)(response);
      
      console.log("🔍 [DEBUG] Raw API response:", result);
      console.log("🔍 [DEBUG] total_metrics:", result.total_metrics);
      console.log("🔍 [DEBUG] metrics_by_status:", result.metrics_by_status);
      console.log("🔍 [DEBUG] namespaces keys:", result.namespaces ? Object.keys(result.namespaces) : "undefined");

      // Handle the response format from metrics_list_handler.py
      let metricsExist = false;
      let count = 0;
      
      if (result.total_metrics !== undefined) {
        // Use the total_metrics count from the API response (preferred)
        metricsExist = result.total_metrics > 0;
        count = result.total_metrics;
        console.log(`📊 [DEBUG] Metrics check via total_metrics: ${count} found`);
      } else if (result.namespaces) {
        // Fallback: count metrics in namespaces
        const totalMetrics = Object.values(result.namespaces)
          .reduce((sum, metrics) => sum + metrics.length, 0);
        metricsExist = totalMetrics > 0;
        count = totalMetrics;
        console.log(`📊 [DEBUG] Metrics check via namespaces: ${count} found`);
      } else if (result.metrics_by_status) {
        // Alternative: use metrics_by_status
        const enabledCount = result.metrics_by_status.enabled || 0;
        const disabledCount = result.metrics_by_status.disabled || 0;
        count = enabledCount + disabledCount;
        metricsExist = count > 0;
        console.log(`📊 [DEBUG] Metrics check via metrics_by_status: ${count} found (${enabledCount} enabled, ${disabledCount} disabled)`);
      }
      
      console.log(`🔍 [DEBUG] Final result: metricsExist=${metricsExist}, count=${count}`);

      setHasMetrics(metricsExist);
      setMetricsCount(count);
      
      console.log(`✅ Metrics check completed: ${metricsExist ? 'found' : 'none'} (${count} total)`);
      
    } catch (error) {
      console.error("❌ Error checking metrics:", error);
      setErrorMessage(
        error instanceof Error 
          ? `Failed to check metrics: ${error.message}` 
          : "Failed to check existing metrics"
      );
      setHasMetrics(false);
      setMetricsCount(0);
    } finally {
      setInitialLoading(false);
    }
  };

  // Check metrics on component mount and when refreshKey changes
  useEffect(() => {
    checkMetricsExist();
  }, [refreshKey]);

  /**
   * Handle discovery start - called by MetricsDiscovery component
   * Sets the discovering state to show appropriate loading UI
   */
  const handleDiscoveryStart = () => {
    console.log("🚀 Discovery started");
    setIsDiscovering(true);
    setErrorMessage(null); // Clear any previous errors
  };

  /**
   * Handle discovery completion - called by MetricsDiscovery component
   * @param success - Whether the discovery completed successfully
   */
  const handleDiscoveryComplete = (success: boolean) => {
    console.log(`🏁 Discovery completed: ${success ? 'success' : 'failed'}`);
    setIsDiscovering(false);
    
    if (success) {
      // Force table refresh by incrementing refreshKey
      // This will trigger MetricsTable to reload data
      setHasMetrics(true);
      setRefreshKey(prev => {
        const newKey = prev + 1;
        console.log(`🔄 Refreshing table with key: ${newKey}`);
        return newKey;
      });
    }
    // Note: Error handling is done within MetricsDiscovery component
    // We don't set error state here to avoid double error messages
  };

  /**
   * Render the appropriate content based on current state
   */
  const renderMainContent = () => {
    // Initial loading - checking if metrics exist
    if (initialLoading) {
      return (
        <LoadingContainer>
          <WaitSpinner size="medium" screenReaderText="Checking existing metrics..." />
          <Typography as="p" style={{ marginTop: '16px' }}>
            Checking for existing metrics...
          </Typography>
        </LoadingContainer>
      );
    }

    // Discovery in progress - show spinner with discovery message
    if (isDiscovering) {
      return (
        <DiscoveryLoadingContainer>
          <WaitSpinner size="medium" screenReaderText="Discovering metrics..." />
          <Typography as="p" style={{ marginTop: '16px', fontWeight: 500 }}>
            Discovering metrics from Confluent Cloud...
          </Typography>
          <Typography as="p" variant="body" style={{ marginTop: '8px', color: '#666' }}>
            This may take a few moments while we fetch the latest metric definitions.
          </Typography>
        </DiscoveryLoadingContainer>
      );
    }

    // No metrics found - show empty state with instructions
    if (!hasMetrics) {
      return (
        <EmptyStateContainer>
          <Typography as="h3" variant="title3" style={{ marginBottom: '16px', color: '#495057' }}>
            No Metrics Configured
          </Typography>
          <Typography as="p" style={{ marginBottom: '8px', color: '#6c757d' }}>
            Start metrics discovery to configure Confluent Cloud metric settings.
          </Typography>
          <Typography as="p" variant="body" style={{ color: '#6c757d' }}>
            Discovery will fetch available metrics from your Confluent Cloud deployment
            and allow you to configure collection settings for each metric.
          </Typography>
        </EmptyStateContainer>
      );
    }

    // Metrics exist - show the full table with optimized loading
    if (hasMetrics) {
      console.log("[DEBUG] Rendering MetricsTable with hasMetrics=true");
      console.log("[DEBUG] refreshKey:", refreshKey);
      console.log("[DEBUG] metricsCount:", metricsCount);
      
      return (
        <div>
          <MetricsTable key={refreshKey} refreshKey={refreshKey} />
        </div>
      );
    }
  };

  return (
    <SplunkThemeProvider family="enterprise" colorScheme="light">
      <ConfigurationContainer>
        {/* Page Header */}
        <Heading level={2} style={{ marginBottom: '8px' }}>
          Confluent Cloud Metrics
        </Heading>
        <Typography as="p" style={{ marginBottom: '24px', color: '#666' }}>
          The Confluent Cloud Metrics provides actionable operational metrics about your Confluent Cloud deployment.
          Discover avialable metrics in order to configure individual metric settings if necessary.
          <br /><br />
          Please see the{" "}
          <a
            href="https://api.telemetry.confluent.cloud/docs/descriptors/datasets/cloud"
            target="_blank"
            rel="noopener noreferrer"
          >
            Metrics Reference
          </a>{" "}
          for a list of available metrics and their settings. The table with per-metric settings below will be used by modular inputs when fetching metrics data.
        </Typography>
        
        {/* Error Message */}
        {errorMessage && (
          <ErrorContainer>
            <Typography as="p" style={{ fontWeight: 500, marginBottom: '4px' }}>
              Configuration Error
            </Typography>
            <Typography as="p" variant="body">
              {errorMessage}
            </Typography>
          </ErrorContainer>
        )}
        
        {/* Discovery Component - Always visible */}
        <MetricsDiscovery 
          onDiscoveryStart={handleDiscoveryStart}
          onDiscoveryComplete={handleDiscoveryComplete}
        />
        
        {/* Main Content Area - State-dependent */}
        {renderMainContent()}
      </ConfigurationContainer>
    </SplunkThemeProvider>
  );
}