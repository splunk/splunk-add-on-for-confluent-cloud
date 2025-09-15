import React, { useState, useEffect, useCallback } from "react";
import Table from "@splunk/react-ui/Table";
import Button from "@splunk/react-ui/Button";
import Dropdown from "@splunk/react-ui/Dropdown";
import Menu from "@splunk/react-ui/Menu";
import Typography from "@splunk/react-ui/Typography";
import Heading from "@splunk/react-ui/Heading";
import Paginator from "@splunk/react-ui/Paginator";
import SidePanel from "@splunk/react-ui/SidePanel";
import Text from "@splunk/react-ui/Text";
import TextArea from "@splunk/react-ui/TextArea";
import Switch from "@splunk/react-ui/Switch";
import WaitSpinner from "@splunk/react-ui/WaitSpinner";
import styled from "styled-components";

import Gear from "@splunk/react-icons/enterprise/Gear";
import Pencil from "@splunk/react-icons/enterprise/Pencil";
import FilterIcon from "@splunk/react-icons/enterprise/Filter";
import CrossIcon from "@splunk/react-icons/Cross";

import * as config from "@splunk/splunk-utils/config";
import { createRESTURL } from "@splunk/splunk-utils/url";
import {
  handleResponse,
  defaultFetchInit,
} from "@splunk/splunk-utils/fetch";

/* ---------- TypeScript Interfaces (ALIGNED) ---------- */
interface MetricData {
  _key: string;
  name: string;
  dataset?: string;
  description?: string;
  type: string;
  unit?: string;
  lifecycle_stage: string;
  labels: string[];
  group_by: string[];
  granularity: string;
  intervals: string[];
  limit: number;
  filter: Record<string, any>;
  enabled: boolean;
  resources?: string
}

//Response format for metric=all from metrics_list_handler
interface MetricsAllResponse {
  metrics: Array<{
    _key: string;
    _user: string;
    name: string;
    description: string;
    type: string;
    unit: string;
    lifecycle_stage: string;
    resources: string;
    labels: string;
    group_by: string;
    granularity: string;
    intervals: string;
    limit: number;
    filter: string;
    enabled: boolean;
    dataset?: string;
  }>;
  total_metrics: number;
  metrics_by_status: {
    enabled: number;
    disabled: number;
  };
  namespaces_summary: Record<string, number>;
}

interface MetricUpdateResponse {
  updated: string;
  fields_updated: string[];
  message?: string;
}

interface MetricUpdateErrorResponse {
  error: string;
  message: string;
  details?: string[];
}

/* ---------- styled components ---------- */
const StyledCloseButton = styled(Button)`
  float: right;
`;

const FormField = styled.div`
  margin-bottom: 16px;
`;

const FormLabel = styled.label`
  display: block;
  margin-bottom: 4px;
  font-weight: 500;
`;

const ButtonContainer = styled.div`
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 24px;
  padding-top: 16px;
  border-top: 1px solid #e0e0e0;
`;

const SpinnerOverlay = styled.div`
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: rgba(255, 255, 255, 0.8);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
`;

const TableContainer = styled.div`
  position: relative;
  overflow-x: auto;
`;

/* ---------- column metadata ---------- */
const columnMeta = [
  { label: "Metric", key: "name", align: "left" as const, width: 600, visible: true },
  { label: "Status", key: "enabled", align: "left" as const, width: 120, visible: true },
  { label: "Resource", key: "resources", align: "left" as const, width: 150, visible: true },
  { label: "Dataset", key: "dataset", align: "left" as const, width: 150, visible: false },
  { label: "Labels", key: "labels", align: "left" as const, width: 450, visible: true },
  { label: "Type", key: "type", align: "left" as const, width: 120, visible: false },
  { label: "Unit", key: "unit", align: "left" as const, width: 80, visible: false },
  { label: "Group by", key: "group_by", align: "left" as const, width: 350, visible: false },
  { label: "Lifecycle", key: "lifecycle_stage", align: "left" as const, width: 180, visible: false },
  { label: "Granularity", key: "granularity", align: "left" as const, width: 120, visible: false },
  { label: "Intervals", key: "intervals", align: "left" as const, width: 250, visible: false },
  { label: "Filter", key: "filter", align: "left" as const, width: 350, visible: false },
  { label: "Limit", key: "limit", align: "right" as const, width: 80, visible: false },
  { label: "Description", key: "description", align: "left" as const, width: 300, visible: false },
];

/* ---------- helpers ---------- */
const Status = ({ enabled }: { enabled: boolean }) => (
  <span style={{ whiteSpace: "nowrap" }}>
    <span
      style={{
        display: "inline-block",
        width: 8,
        height: 8,
        borderRadius: "50%",
        marginRight: 6,
        backgroundColor: enabled ? "#2ecc71" : "#95a5a6",
        verticalAlign: "middle",
      }}
    />
    {enabled ? "Enabled" : "Disabled"}
  </span>
);

const renderCell = (row: MetricData, key: string) => {
  if (key === "enabled") return <Status enabled={row.enabled} />;
  if (key === "resources") {
    return row.resources || "unknown";
  }
  if (key === "dataset") {
    return row.dataset || "cloud";
  }
  
  const value = row[key as keyof MetricData];
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return value?.toString() ?? "—";
};

/* ---------- side panel component ---------- */
const MetricEditPanel = ({ 
  isOpen, 
  onClose, 
  metric, 
  onSave 
}: {
  isOpen: boolean;
  onClose: () => void;
  metric: MetricData | null;
  onSave: (metric: MetricData) => Promise<void>;
}) => {
  const [formData, setFormData] = useState({
    granularity: "",
    intervals: "",
    group_by: "",
    filter: "",
    limit: "",
    enabled: false,
  });
  const [isSaving, setIsSaving] = useState(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);

  useEffect(() => {
    if (metric) {
      setFormData({
        granularity: metric.granularity || "PT6H",
        intervals: Array.isArray(metric.intervals) 
          ? metric.intervals.join(", ") 
          : String(metric.intervals || "now-1d|d/now"),
        group_by: Array.isArray(metric.group_by) 
          ? metric.group_by.join(", ") 
          : String(metric.group_by || ""),
        filter: typeof metric.filter === "object" && metric.filter !== null
          ? (Object.keys(metric.filter).length > 0 ? JSON.stringify(metric.filter, null, 2) : "")
          : String(metric.filter || ""),
        limit: metric.limit?.toString() || "100",
        enabled: metric.enabled || false,
      });
      setValidationErrors([]);
    }
  }, [metric]);

  const handleInputChange = (field: string, value: string | boolean) => {
    setFormData(prev => ({
      ...prev,
      [field]: value
    }));
    setValidationErrors([]);
  };

  const validateFormData = (): string[] => {
    const errors: string[] = [];
    
    // Validate granularity (required)
    if (!formData.granularity.trim()) {
      errors.push("Granularity is required");
    } else {
      const validGranularities = ["PT1M", "PT5M", "PT15M", "PT30M", "PT1H", "PT4H", "PT6H", "PT12H", "P1D", "ALL"];
      if (!validGranularities.includes(formData.granularity.trim())) {
        errors.push(`Invalid granularity. Supported values: ${validGranularities.join(", ")}`);
      }
    }
    
    // Validate intervals (required, must have exactly one interval)
    if (!formData.intervals.trim()) {
      errors.push("Intervals is required");
    } else {
      const intervalArray: string[] = formData.intervals
      .split(",")
      .map((s: string) => s.trim())
      .filter((s: string) => s.length > 0);
      if (intervalArray.length !== 1) {
        errors.push("Intervals must contain exactly one interval");
      } else {
        const intervalValue: string = intervalArray[0];
        const intervalPattern: RegExp = /^(now|[\d\-T:Z+\-]+)([+\-]\d+[mhd])*(\|[mhd])?\/?(now|[\d\-T:Z+\-]+)([+\-]\d+[mhd])*(\|[mhd])?$|^PT\d+[MH]\/now$|^now[+\-]\d+[mhd](\|[mhd])?\/now(\|[mhd])?$/;

        if (!intervalPattern.test(intervalValue)) {
          errors.push("Invalid interval format. Expected ISO-8601 interval format like 'now-1h|h/now|h'");
        }
      }
    }

    // Validate group_by (required, cannot be empty)
    // Not required
    
    // Validate filter (required, cannot be empty)
    if (!formData.filter.trim()) {
      errors.push("Filter is required and cannot be empty - must be a valid filter object");
    } else {
      try {
        const parsedFilter = JSON.parse(formData.filter);
        if (typeof parsedFilter !== "object" || parsedFilter === null || Object.keys(parsedFilter).length === 0) {
          errors.push("Filter cannot be empty - must be a valid filter object");
        }
        // Basic filter structure validation
        if (!parsedFilter.op) {
          errors.push("Filter missing required 'op' field");
        } else {
          const validOps = ["EQ", "GT", "GTE", "AND", "OR", "NOT"];
          if (!validOps.includes(parsedFilter.op)) {
            errors.push(`Invalid filter operator. Valid operators: ${validOps.join(", ")}`);
          }
        }
      } catch (e) {
        errors.push("Invalid JSON format in Filter field");
      }
    }
    
    // Validate limit (must be 1-1000)
    if (!formData.limit.trim()) {
      errors.push("Limit is required");
    } else {
      const limitNum: number = parseInt(formData.limit, 10);
      if (isNaN(limitNum)) {
        errors.push("Limit must be a valid number");
      } else if (limitNum < 1 || limitNum > 1000) {
        errors.push("Limit must be between 1 and 1000");
      }
    }
    
    return errors;
  };

  const handleSave = async () => {
    if (!metric) return;

    // Validate form data before submitting
    const errors = validateFormData();
    if (errors.length > 0) {
      setValidationErrors(errors);
      return;
    }
    
    setIsSaving(true);
    setValidationErrors([]);
    
    try {
      let parsedFilter;
      try {
        parsedFilter = formData.filter.trim() ? JSON.parse(formData.filter) : {};
      } catch (e) {
        throw new Error("Invalid JSON format in Filter field");
      }


      const updatePayload = {
        _key: metric._key,
        metric: metric.name,                                                    // string
        granularity: formData.granularity.trim(),                             // string  
        intervals: [formData.intervals.trim()], // array of strings
        group_by: formData.group_by.split(",").map(s => s.trim()).filter(s => s),   // array of strings
        filter: parsedFilter,                                                  // object
        limit: parseInt(formData.limit) || 100,                              // number
        enabled: formData.enabled,                                           // boolean
      };

      console.log("🔄 Sending update payload:", updatePayload);

      const updatedMetric: MetricData = {
        _key: metric._key,
        name: metric.name,
        description: metric.description,
        type: metric.type,
        unit: metric.unit,
        lifecycle_stage: metric.lifecycle_stage,
        labels: metric.labels,
        resources: metric.resources,
        dataset: metric.dataset,
        // Update editable fields from form
        granularity: formData.granularity.trim(),
        intervals: formData.intervals.split(",").map(s => s.trim()).filter(s => s),
        group_by: formData.group_by.split(",").map(s => s.trim()).filter(s => s),
        filter: parsedFilter,
        limit: parseInt(formData.limit) || 100,
        enabled: formData.enabled,
      };
      
      await onSave(updatedMetric);
      onClose();
    } catch (error) {
      console.error("Save error:", error);
      if (error instanceof Error) {
        setValidationErrors([error.message]);
      } else {
        setValidationErrors(["Failed to save metric settings"]);
      }
    } finally {
      setIsSaving(false);
    }
  };

  if (!metric) return null;

  const headerStyle: React.CSSProperties = {
    width: '100%',
    padding: '16px',
    boxSizing: 'border-box',
    display: 'block',
    position: 'relative',
    borderBottom: '1px solid #e0e0e0',
  };

  const contentStyle: React.CSSProperties = {
    padding: '16px',
    flex: '1 0 0px',
    overflowY: 'auto',
  };

  return (
    <SidePanel
      dockPosition="right"
      open={isOpen}
      onRequestClose={onClose}
      useLayerForClickAway={false}
      innerStyle={{ 
        width: 450, 
        display: 'flex', 
        flexDirection: 'column' 
      }}
    >
      <div style={headerStyle}>
        <StyledCloseButton
          appearance="subtle"
          onClick={onClose}
          icon={
            <span style={{ display: 'inline-block', width: 20, height: 20 }}>
              <CrossIcon />
            </span>
          }
        />
        <Heading level={4} style={{ marginTop: 0, marginBottom: 8 }}>
          {metric.name}
        </Heading>
        <Typography as="p" variant="body" style={{ color: "#666" }}>
          {metric.description || "Configure settings for this Confluent Cloud metric."}
        </Typography>
      </div>

      <div style={contentStyle}>
        {/* Validation Errors Display */}
        {validationErrors.length > 0 && (
          <div style={{ 
            marginBottom: '16px', 
            padding: '12px', 
            backgroundColor: '#ffebee', 
            border: '1px solid #f44336',
            borderRadius: '4px'
          }}>
            <Typography as="p" variant="body" style={{ color: '#c62828', fontWeight: 'bold', marginBottom: '8px' }}>
              Validation Errors:
            </Typography>
            <ul style={{ margin: 0, paddingLeft: '20px', color: '#c62828' }}>
              {validationErrors.map((error, index) => (
                <li key={index}>{error}</li>
              ))}
            </ul>
          </div>
        )}

        {/* Dataset Field - Non-editable */}
        <FormField>
          <FormLabel>Dataset:</FormLabel>
          <Text
            value={metric?.dataset || 'cloud'}
            disabled
            onChange={() => {}}
            style={{ 
              backgroundColor: '#f8f9fa', 
              cursor: 'not-allowed',
              color: '#6c757d'
            }}
          />
          <Typography as="p" variant="body" style={{ color: "#666", marginTop: 4 }}>
            The dataset this metric belongs to (read-only)
          </Typography>
        </FormField>
        
        <FormField>
          <FormLabel>
            Granularity <span style={{ color: '#d32f2f' }}>*</span>
          </FormLabel>
          <Text
            value={formData.granularity}
            onChange={(_, { value }) => handleInputChange('granularity', value)}
            placeholder="PT6H"
            disabled={isSaving}
          />
          <Typography as="p" variant="body" style={{ color: "#666", marginTop: 4 }}>
            Valid values: PT1M, PT5M, PT15M, PT30M, PT1H, PT4H, PT6H, PT12H, P1D, ALL
          </Typography>
        </FormField>

        <FormField>
          <FormLabel>
            Intervals <span style={{ color: '#d32f2f' }}>*</span>
          </FormLabel>
          <Text
            value={formData.intervals}
            onChange={(_, { value }) => handleInputChange('intervals', value)}
            placeholder="now-1d|d/now"
            disabled={isSaving}
          />
          <Typography as="p" variant="body" style={{ color: "#666", marginTop: 4 }}>
            Comma-separated intervals in ISO-8601 format
          </Typography>
        </FormField>

        <FormField>
          <FormLabel>Group by</FormLabel>
          <Text
            value={formData.group_by}
            onChange={(_, { value }) => handleInputChange('group_by', value)}
            placeholder="metric.topic, metric.partition"
            disabled={isSaving}
          />
          <Typography as="p" variant="body" style={{ color: "#666", marginTop: 4 }}>
            Comma-separated list of labels to group by
          </Typography>
        </FormField>

        <FormField>
          <FormLabel>
            Filter <span style={{ color: '#d32f2f' }}>*</span>
          </FormLabel>
          <TextArea
            value={formData.filter}
            onChange={(_, { value }) => handleInputChange('filter', value)}
            disabled={isSaving}
            style={{ minHeight: '80px' }}
          />
          <Typography as="p" variant="body" style={{ color: "#666", marginTop: 4 }}>
            JSON filter object. Example: {`{"field":"resource.kafka.id","op":"EQ","value":"lkc-abc123"}`}
          </Typography>
        </FormField>

        <FormField>
          <FormLabel>Limit</FormLabel>
          <Text
            value={formData.limit}
            onChange={(_, { value }) => handleInputChange('limit', value)}
            placeholder="100"
            disabled={isSaving}
          />
          <Typography as="p" variant="body" style={{ color: "#666", marginTop: 4 }}>
            Maximum number of data points to return (1-1000)
          </Typography>
        </FormField>

        <FormField>
          <FormLabel>Enabled</FormLabel>
          <Switch
            selected={formData.enabled}
            onClick={() => handleInputChange('enabled', !formData.enabled)}
            appearance="toggle"
            disabled={isSaving}
          />
          <Typography as="p" variant="body" style={{ color: "#666", marginTop: 4 }}>
            Enable or disable this metric for collection
          </Typography>
        </FormField>

        <ButtonContainer>
          <Button
            label="Cancel"
            onClick={onClose}
            inline={false}
            disabled={isSaving}
          />
          <Button
            label={isSaving ? "Saving..." : "Save"}
            appearance="primary"
            onClick={handleSave}
            inline={false}
            disabled={isSaving}
          />
        </ButtonContainer>
      </div>
    </SidePanel>
  );
};

/* ---------- main component ---------- */
interface MetricsTableProps {
  refreshKey?: number;
}

export default function MetricsTable({ refreshKey = 0 }: MetricsTableProps) {
  console.log("[DEBUG] MetricsTable component is MOUNTING!");
  console.log("[DEBUG] MetricsTable refreshKey:", refreshKey);
  const [headers, setHeaders] = useState(columnMeta);
  const [rows, setRows] = useState<MetricData[]>([]);
  const [sortKey, setSortKey] = useState<keyof MetricData>("name");
  const [sortDir, setSortDir] = useState<"asc" | "desc" | "none">("asc");
  const [statusFilter, setStatusFilter] = useState("All");
  const [resourceFilter, setResourceFilter] = useState("All");
  const [datasetFilter, setDatasetFilter] = useState("All"); 
  const [isLoading, setIsLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const [pageSize, setPageSize] = useState(10);
  const [page, setPage] = useState(1);
  const [isPageLoading, setIsPageLoading] = useState(false);

  const [sidePanelOpen, setSidePanelOpen] = useState(false);
  const [selectedMetric, setSelectedMetric] = useState<MetricData | null>(null);

  // NEW: Optimized single-call metrics loading using metric=all
  const loadMetrics = useCallback(async () => {
    console.log("[DEBUG] loadMetrics() function is EXECUTING!");
    console.log("[DEBUG] About to make API call with metric=all");

    try {
      setIsLoading(true);
      setErrorMessage(null);
      
      console.log("Loading all metrics with details in single API call...");
      
      // Use the new metric=all parameter for efficient loading
      const baseUrl = createRESTURL("confluent_metrics/list", {
        app: config.app,
        sharing: "app"
      });

      const allMetricsUrl = `${baseUrl}?metric=all`;
      console.log("[DEBUG] Base URL from createRESTURL:", baseUrl);
      console.log("[DEBUG] Final URL with query params:", allMetricsUrl);

      const response = await fetch(allMetricsUrl, {
        ...defaultFetchInit,
        method: "GET",
        headers: {
          "X-Splunk-Form-Key": config.CSRFToken,
          "X-Requested-With": "XMLHttpRequest",
        },
      });

      const result: MetricsAllResponse = await handleResponse(200)(response);

      console.log("[DEBUG] Raw API response from /list?metric=all:", result);
      console.log("[DEBUG] Response structure keys:", Object.keys(result));
      console.log("[DEBUG] Metrics array length:", result.metrics?.length);
      console.log("[DEBUG] Total metrics reported:", result.total_metrics);
    
      if (!result.metrics || !Array.isArray(result.metrics)) {
        console.log("No metrics array found in response");
        setRows([]);
        return;
      }

      // Process and transform the metrics data
      const metrics = result.metrics;
      console.log(`✅ Successfully loaded ${metrics.length} metrics with full details`);

      // Transform the raw API data to match our MetricData interface
      const processedMetrics: MetricData[] = metrics.map(metric => {
      console.log(`[DEBUG] Processing metric: ${metric.name}`);
      
      return {
        _key: metric._key || metric.name,
        name: metric.name || '',
        dataset: metric.dataset || 'cloud',
        description: metric.description || '',
        type: metric.type || 'GAUGE',
        unit: metric.unit || '',
        lifecycle_stage: metric.lifecycle_stage || 'GENERAL_AVAILABILITY',
        
        
        // Handle both string and array formats for labels
        labels: (() => {
          if (Array.isArray(metric.labels)) {
            return metric.labels;
          }
        return metric.labels 
          ? metric.labels.split(',').map((s: string) => s.trim()).filter(s => s)
          : [];
        })(),
          
        // Handle both string and array formats for group_by
        group_by: (() => {
          if (Array.isArray(metric.group_by)) {
            return metric.group_by;
          }
          return metric.group_by
            ? metric.group_by.split(',').map((s: string) => s.trim()).filter(s => s)
            : [];
        })(),

        granularity: metric.granularity || 'PT6H',
        
        // Handle both string and array formats for intervals
        intervals: (() => {
          if (Array.isArray(metric.intervals)) {
            return metric.intervals;
          }
        return metric.intervals
          ? metric.intervals.split(',').map((s: string) => s.trim()).filter(s => s)
          : ['now-1d|d/now'];
        })(),
          
        limit: typeof metric.limit === 'number' ? metric.limit : (parseInt(String(metric.limit)) || 100),
        
        // Handle both string and object formats for filter
        filter: (() => {
          if (typeof metric.filter === 'object' && metric.filter !== null) {
            return metric.filter;
          }
        try {
          return metric.filter && metric.filter.trim() 
            ? JSON.parse(metric.filter) 
            : {};
        } catch (e) {
          console.warn(`[DEBUG] Invalid JSON filter for ${metric.name}:`, metric.filter);
          return {};
        }
    })(),
    
        
        enabled: Boolean(metric.enabled),
        resources: metric.resources || '',
      };
    });

      setRows(processedMetrics);
      
      // Log summary for debugging
      const enabledCount = processedMetrics.filter(m => m.enabled).length;
      console.log(`📊 Metrics summary: ${processedMetrics.length} total, ${enabledCount} enabled, ${processedMetrics.length - enabledCount} disabled`);
      
    } catch (error) {
      console.error("❌ Failed to load metrics:", error);
      setErrorMessage(
        error instanceof Error 
          ? `Failed to load metrics: ${error.message}` 
          : "Failed to load metrics from server"
      );
      setRows([]);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    console.log("[DEBUG] MetricsTable useEffect triggered!");
    console.log("[DEBUG] About to call loadMetrics()");
    loadMetrics();
  }, [loadMetrics, refreshKey]);

  const handleMetricUpdate = async (updatedMetric: MetricData): Promise<void> => {
    try {
      const url = createRESTURL("confluent_metrics/update", {
        app: config.app,
        sharing: "app",
      });

      const updatePayload = {
        _key: updatedMetric._key,
        metric: updatedMetric.name,
        granularity: updatedMetric.granularity,
        intervals: Array.isArray(updatedMetric.intervals) 
        ? updatedMetric.intervals
        : [String(updatedMetric.intervals)],
        group_by: Array.isArray(updatedMetric.group_by) 
        ? updatedMetric.group_by
        : String(updatedMetric.group_by).split(',')
          .map(s => s.trim()).filter(Boolean),
        filter: updatedMetric.filter,
        limit: updatedMetric.limit,
        enabled: updatedMetric.enabled,
      };

      console.log("🔄 [DEBUG] Sending PUT request to:", url);
      console.log("🔄 [DEBUG] Update payload:", JSON.stringify(updatePayload, null, 2));

      const response = await fetch(url, {
        ...defaultFetchInit,
        method: "PUT",
        headers: {
          "X-Splunk-Form-Key": config.CSRFToken,
          "X-Requested-With": "XMLHttpRequest",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(updatePayload),
      });

      if (!response.ok) {
        const errorResult: MetricUpdateErrorResponse = await response.json();
        console.error("❌ [DEBUG] Update failed:", errorResult);
        throw new Error(errorResult.message || `HTTP ${response.status}: ${response.statusText}`);
      }

      const result: MetricUpdateResponse = await handleResponse(200)(response);
      console.log("✅ [DEBUG] Update successful:", result);
      
      if (result.updated) {
      // Update local state with the new metric data
      setRows(prev => 
        prev.map(row => 
          row.name === updatedMetric._key ? updatedMetric : row
        )
      );
      console.log(`✅ Updated metric: ${result.updated}, fields: [${result.fields_updated.join(", ")}]`);
    } else {
      throw new Error("Invalid response format from update handler");
    }
  } catch (error) {
    console.error("❌ Failed to update metric:", error);
    throw error;
  }
};

  const toggleColumn = useCallback((_: React.MouseEvent, { label }: { label: string }) => {
    setHeaders((prev) =>
      prev.map((h) => (h.label === label ? { ...h, visible: !h.visible } : h))
    );
  }, []);

  const handleSort = useCallback(
    (
      _event: React.MouseEvent<Element, MouseEvent> | React.KeyboardEvent<HTMLButtonElement | HTMLDivElement>, 
      data: { columnId?: string; id?: string; index: number; sortDir: "asc" | "desc" | "none"; sortKey?: string }
    ) => {
      const newKey = data.sortKey as keyof MetricData;
      if (!newKey) return;
      
      if (sortKey === newKey) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
      else {
        setSortKey(newKey);
        setSortDir("asc");
      }
    },
    [sortKey]
  );

  const handleResizeColumn = useCallback((
    _event: MouseEvent | React.KeyboardEvent<HTMLHRElement> | React.MouseEvent<Element, MouseEvent>, 
    data: { columnId?: string; width: number }
  ) => {
    const { columnId, width } = data;
    if (!columnId || !["labels", "group_by", "filter"].includes(columnId)) return;
    setHeaders((prev) =>
      prev.map((h) =>
        h.key === columnId ? { ...h, width: Math.max(width, 100) } : h
      )
    );
  }, []);

  const handleEditClick = ({ data }: { data: MetricData }) => {
    setSelectedMetric(data);
    setSidePanelOpen(true);
  };

  const handleSidePanelClose = () => {
    setSidePanelOpen(false);
    setSelectedMetric(null);
  };

  const handlePageChange = (
    _event: React.KeyboardEvent<HTMLInputElement> | React.MouseEvent<HTMLButtonElement, MouseEvent>, 
    { page: newPage }: { page: number }
  ) => {
    setIsPageLoading(true);
    
    setTimeout(() => {
      setPage(newPage);
      setIsPageLoading(false);
    }, 500);
  };

  const handlePageSizeChange = (newSize: number) => {
    setIsPageLoading(true);
    
    setTimeout(() => {
      setPageSize(newSize);
      setPage(1);
      setIsPageLoading(false);
    }, 300);
  };

  const statusOpts = ["All", "Enabled", "Disabled"];
  
  const resourceOpts = [
    "All",
    ...Array.from(new Set(
    rows.map((r) => r.resources).filter((r): r is string => r != null && r.trim() !== "")
    )).sort(),
  ];

  const datasetOpts = [
    "All",
    ...Array.from(new Set(
    rows.map((r) => r.dataset).filter((d): d is string => d != null && d.trim() !== "")
    )).sort(),
  ];

  const filtered = rows.filter((m) => {
    const statusMatch = 
      statusFilter === "All" ||
      (statusFilter === "Enabled" && m.enabled) ||
      (statusFilter === "Disabled" && !m.enabled);
    
    const resourceMatch = resourceFilter === "All" || m.resources === resourceFilter;
    const datasetMatch = datasetFilter === "All" || m.dataset === datasetFilter;

    return statusMatch && resourceMatch && datasetMatch;
  });

  const sorted = [...filtered].sort((a, b) => {
    if (sortDir === "none") return 0;
    
    const aVal = a[sortKey];
    const bVal = b[sortKey];
    
    if (aVal === bVal) return 0;
    
    if (typeof aVal === "string" && typeof bVal === "string") {
      return sortDir === "asc" ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
    }
    
    if (aVal === undefined && bVal === undefined) return 0;
    if (aVal === undefined) return sortDir === "asc" ? -1 : 1;
    if (bVal === undefined) return sortDir === "asc" ? 1 : -1;
    
    return sortDir === "asc" ? (aVal > bVal ? 1 : -1) : (aVal < bVal ? 1 : -1);
  });

  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const pageRows = sorted.slice((page - 1) * pageSize, page * pageSize);

  const gearDropdown = (
    <Dropdown toggle={<Button appearance="subtle" icon={<Gear />} />}>
      <Menu>
        <Menu.Heading>Show / Hide Columns</Menu.Heading>
        {headers.map((h) => (
          <Menu.Item
            key={h.key}
            selectable
            selected={h.visible}
            onClick={(event: React.MouseEvent) => toggleColumn(event, { label: h.label })}
          >
            {h.label}
          </Menu.Item>
        ))}
      </Menu>
    </Dropdown>
  );

  const sizeDropdown = (
    <Dropdown
      toggle={
        <Button
          isMenu
          label={`${pageSize} items per page`}
          inline
          style={{ width: "auto", maxWidth: 160, textAlign: "left" }}
        />
      }
    >
      <Menu>
        {[5, 10, 20, 50].map((n) => (
          <Menu.Item
            key={n}
            selectable
            selected={pageSize === n}
            onClick={() => handlePageSizeChange(n)}
          >
            {n} items per page
          </Menu.Item>
        ))}
      </Menu>
    </Dropdown>
  );

  if (isLoading) {
    return (
      <div style={{ textAlign: 'center', padding: '40px' }}>
        <WaitSpinner size="medium" screenReaderText="Loading metrics..." />
        <Typography as="p" style={{ marginTop: '16px' }}>
          Loading metrics...
        </Typography>
      </div>
    );
  }

  if (errorMessage) {
    return (
      <div style={{ textAlign: 'center', padding: '40px' }}>
        <Typography as="p" style={{ color: '#d32f2f', marginBottom: '8px' }}>
          Error Loading Metrics
        </Typography>
        <Typography as="p" variant="body">
          {errorMessage}
        </Typography>
      </div>
    );
  }

  if (rows.length === 0) {
    return (
      <div style={{ textAlign: 'center', padding: '40px' }}>
        <Typography as="p">
          No metrics found. Please run metrics discovery first.
        </Typography>
      </div>
    );
  }

  return (
    <div>
      <div style={{ display: "inline-block", maxWidth: "100%" }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: 12,
            marginTop: 12,
          }}
        >
          <div style={{ flex: 1 }} />
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            {sizeDropdown}
            <Paginator.PageControl
              current={page}
              totalPages={totalPages}
              onChange={handlePageChange}
            />
          </div>
        </div>

        <TableContainer>
          {isPageLoading && (
            <SpinnerOverlay>
              <WaitSpinner 
                size="medium" 
                screenReaderText="Loading page data..."
              />
            </SpinnerOverlay>
          )}
          <Table
            headType="fixed"
            stripeRows
            onRequestResizeColumn={handleResizeColumn}
            style={{ 
              marginTop: 16, 
              tableLayout: "auto", 
              whiteSpace: "nowrap",
              opacity: isPageLoading ? 0.5 : 1,
              pointerEvents: isPageLoading ? "none" : "auto"
            }}
          >
            <Table.Head>
              {headers
                .filter((h) => h.visible)
                .map((h) => {
                  if (h.key === "enabled") {
                    return (
                      <Table.HeadDropdownCell
                        key={h.key}
                        columnId={h.key}
                        align={h.align}
                        width={h.width}
                        resizable={false}
                        label={
                          <>
                            {h.label}
                            {statusFilter !== "All" && (
                              <FilterIcon style={{ marginLeft: 4 }} />
                            )}
                          </>
                        }
                      >
                        <Menu>
                          <Menu.Heading>Filter Status</Menu.Heading>
                          {statusOpts.map((opt) => (
                            <Menu.Item
                              key={opt}
                              selectable
                              selected={statusFilter === opt}
                              onClick={() => setStatusFilter(opt)}
                            >
                              {opt}
                            </Menu.Item>
                          ))}
                        </Menu>
                      </Table.HeadDropdownCell>
                    );
                  }
                  if (h.key === "resources") {
                    return (
                      <Table.HeadDropdownCell
                        key={h.key}
                        columnId={h.key}
                        align={h.align}
                        width={h.width}
                        resizable={false}
                        label={
                          <>
                            {h.label}
                            {resourceFilter !== "All" && (
                              <FilterIcon style={{ marginLeft: 4 }} />
                            )}
                          </>
                        }
                      >
                        <Menu>
                          <Menu.Heading>Filter Resource</Menu.Heading>
                          {resourceOpts.map((opt) => (
                            <Menu.Item
                              key={opt}
                              selectable
                              selected={resourceFilter === opt}
                              onClick={() => setResourceFilter(opt)}
                            >
                              {opt}
                            </Menu.Item>
                          ))}
                        </Menu>
                      </Table.HeadDropdownCell>
                    );
                  }
                  if (h.key === "dataset") {
                    return (
                      <Table.HeadDropdownCell
                        key={h.key}
                        columnId={h.key}
                        align={h.align}
                        width={h.width}
                        resizable={false}
                        label={
                          <>
                            {h.label}
                            {datasetFilter !== "All" && (
                              <FilterIcon style={{ marginLeft: 4 }} />
                            )}
                          </>
                        }
                      >
                        <Menu>
                          <Menu.Heading>Filter Dataset</Menu.Heading>
                          {datasetOpts.map((opt) => (
                            <Menu.Item
                              key={opt}
                              selectable
                              selected={datasetFilter === opt}
                              onClick={() => setDatasetFilter(opt)}
                            >
                              {opt}
                            </Menu.Item>
                          ))}
                        </Menu>
                      </Table.HeadDropdownCell>
                    );
                  }
                  if (["labels", "group_by", "filter"].includes(h.key)) {
                    return (
                      <Table.HeadCell
                        key={h.key}
                        columnId={h.key}
                        align={h.align}
                        width={h.width}
                        resizable
                      >
                        {h.label}
                      </Table.HeadCell>
                    );
                  }
                  return (
                    <Table.HeadCell
                      key={h.key}
                      align={h.align}
                      width={h.width}
                      resizable={false}
                      sortKey={h.key}
                      sortDir={h.key === sortKey ? sortDir : "none"}
                      onSort={h.key === "name" ? handleSort : undefined}
                    >
                      {h.label}
                    </Table.HeadCell>
                  );
                })}
              <Table.HeadCell
                key="gear"
                width={70}
                align="center"
                resizable={false}
              >
                {gearDropdown}
              </Table.HeadCell>
            </Table.Head>

            <Table.Body>
              {pageRows.map((row) => (
                <Table.Row key={row._key || row.name} data={row}>
                  {headers
                    .filter((h) => h.visible)
                    .map((h) => (
                      <Table.Cell key={`${row.name}-${h.key}`} align={h.align}>
                        {renderCell(row, h.key)}
                      </Table.Cell>
                    ))}
                  <Table.Cell
                    key={`${row.name}-edit`}
                    align="center"
                  >
                    <Button
                      appearance="subtle"
                      icon={<Pencil />}
                      onClick={() => handleEditClick({ data: row })}
                      disabled={isPageLoading}
                    />
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table>
        </TableContainer>

        <div
          style={{ display: "flex", justifyContent: "flex-end", marginTop: 12 }}
        >
          <Paginator
            aria-label="Metrics pages"
            current={page}
            totalPages={totalPages}
            onChange={handlePageChange}
            alwaysShowLastPageLink
          />
        </div>
      </div>

      <MetricEditPanel
        isOpen={sidePanelOpen}
        onClose={handleSidePanelClose}
        metric={selectedMetric}
        onSave={handleMetricUpdate}
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
}