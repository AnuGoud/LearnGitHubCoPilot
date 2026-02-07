using DocumentProcessing.Models;
using DocumentProcessing.Activities;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Azure.Functions.Worker;

namespace DocumentProcessing.Orchestrations
{
    /// <summary>
    /// Durable Functions orchestrator for document processing.
    /// 
    /// Orchestration flow:
    /// 1. Receive input: blob path of the incoming PDF.
    /// 2. Call activity to split the PDF into segments on black pages.
    /// 3. Call activity to compute SHA-256 and perceptual hashes for each segment.
    /// 4. Call activity to perform deduplication (compare against index).
    /// 5. Call activity to store duplicate segments in duplicates container.
    /// 6. Call activity to store results and summaries.
    /// 7. Return processing summary.
    /// 
    /// This orchestrator follows Durable Functions best practices:
    /// - Deterministic: no dynamic decision-making based on external state.
    /// - All activities are called asynchronously.
    /// - Errors are propagated naturally and logged by the DurableTask runtime.
    /// </summary>
    public class DocumentProcessingOrchestrator
    {
        [Function(nameof(DocumentProcessingOrchestrator))]
        public async Task<ProcessingSummary> RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context,
            ProcessingRequest request)
        {
            // Track timing for business metrics
            var startTime = context.CurrentUtcDateTime;

            // Step 1: Split the document.
            var splitStartTime = context.CurrentUtcDateTime;
            var splitResult = await context.CallActivityAsync<SplitResult>(
                nameof(SplitDocumentActivity),
                request
            );
            var splitDuration = context.CurrentUtcDateTime - splitStartTime;

            // Step 2: Compute hashes for all segments.
            // IMPORTANT: Keep OutputBlobPath as it's needed by DeduplicationActivity and StoreDuplicatesActivity
            // to locate segment PDFs in storage for copying duplicates
            var cleanSegments = splitResult.Segments.Select(s => new DocumentSegment
            {
                SegmentId = s.SegmentId,
                SourceFileName = s.SourceFileName,
                StartPageNumber = s.StartPageNumber,
                EndPageNumber = s.EndPageNumber,
                PageCount = s.PageCount,
                OutputBlobPath = s.OutputBlobPath, // ✅ KEEP: Required by dedup activity for reading segments
                SizeBytes = s.SizeBytes,
                CreatedAt = s.CreatedAt,
                SegmentBytes = null, // ⚠️ CLEAR: byte[] cannot be JSON serialized in Durable Functions
                DocumentType = s.DocumentType,
                DocumentTypeConfidence = s.DocumentTypeConfidence,
                BoundaryConfidence = s.BoundaryConfidence,
                DetectedAsDuplicate = s.DetectedAsDuplicate, // ✅ KEEP: Required for within-batch duplicate detection
                DuplicateOfSegmentIndex = s.DuplicateOfSegmentIndex // ✅ KEEP: Required for within-batch duplicate detection
            }).ToList();
            
            var cleanSplitResult = new SplitResult
            {
                BatchId = request.BatchId,
                Segments = cleanSegments,
                OriginalPdfBytes = null  // Don't pass large PDF bytes - only segments needed
            };
            
            var hashResults = await context.CallActivityAsync<List<HashResult>>(
                nameof(ComputeHashesActivity),
                cleanSplitResult
            );

            // Step 3: Perform deduplication.
            var dedupStartTime = context.CurrentUtcDateTime;
            var dedupResults = await context.CallActivityAsync<List<DeduplicationResult>>(
                nameof(DeduplicationActivity),
                hashResults
            );
            var dedupDuration = context.CurrentUtcDateTime - dedupStartTime;

            // Step 4: Store duplicates in separate container.
            var duplicatesSummary = await context.CallActivityAsync<StoreDuplicatesSummary>(
                nameof(StoreDuplicatesActivity),
                new StoreDuplicatesRequest
                {
                    BatchId = request.BatchId,
                    DedupResults = dedupResults
                }
            );

            // Step 5: Store results.
            // Clean segments again before storing (remove OutputBlobPath)
            var cleanSegmentsForStorage = splitResult.Segments.Select(s => new DocumentSegment
            {
                SegmentId = s.SegmentId,
                SourceFileName = s.SourceFileName,
                StartPageNumber = s.StartPageNumber,
                EndPageNumber = s.EndPageNumber,
                PageCount = s.PageCount,
                OutputBlobPath = null,
                SizeBytes = s.SizeBytes,
                CreatedAt = s.CreatedAt
            }).ToList();
            
            var summary = await context.CallActivityAsync<ProcessingSummary>(
                nameof(StoreResultsActivity),
                new StoreResultsRequest
                {
                    BatchId = request.BatchId,
                    SplitSegments = cleanSegmentsForStorage,
                    DedupResults = dedupResults
                }
            );

            // Step 6: Sync to SharePoint (best-effort, non-blocking)
            // This runs after all critical blob operations complete
            // ✅ FIX #3: Proper null handling + preserve within-batch duplicate detection
            var segmentsForSharePoint = splitResult.Segments.Select(seg =>
            {
                var dedupResult = dedupResults.FirstOrDefault(d => d.SegmentId == seg.SegmentId);

                // Merge dedup results with segment data
                // IMPORTANT: Preserve DetectedAsDuplicate and DuplicateOfSegmentIndex from split activity
                var isExactDup = dedupResult?.IsExactDuplicate ?? false;
                var isSimilar = dedupResult?.IsSimilar ?? false;
                var exactDupOf = dedupResult?.ExactDuplicateOf;
                var minHamming = dedupResult?.MinHammingDistance ?? 0;

                // If not detected by dedup service, check if it was detected during split
                var isDetectedDup = seg.DetectedAsDuplicate;
                var dupOfIndex = seg.DuplicateOfSegmentIndex;

                return new DocumentSegment
                {
                    SegmentId = seg.SegmentId,
                    SourceFileName = seg.SourceFileName,
                    StartPageNumber = seg.StartPageNumber,
                    EndPageNumber = seg.EndPageNumber,
                    PageCount = seg.PageCount,
                    OutputBlobPath = seg.OutputBlobPath,  // Keep this for SharePoint sync
                    SizeBytes = seg.SizeBytes,
                    CreatedAt = seg.CreatedAt,
                    DocumentType = seg.DocumentType,
                    IsExactDuplicate = isExactDup,
                    IsSimilar = isSimilar,
                    ExactDuplicateOf = exactDupOf,
                    MinHammingDistance = minHamming,
                    DetectedAsDuplicate = isDetectedDup,  // ✅ PRESERVE: Within-batch duplicate flag
                    DuplicateOfSegmentIndex = dupOfIndex  // ✅ PRESERVE: Index of original segment
                };
            }).ToList();

            var sharePointStartTime = context.CurrentUtcDateTime;
            var sharePointSummary = await context.CallActivityAsync<SharePointSyncSummary>(
                nameof(SyncToSharePointActivity),
                new SharePointSyncRequest
                {
                    BatchId = request.BatchId,
                    Segments = segmentsForSharePoint,
                    SkipDuplicates = false  // Upload all documents (unique and duplicates to separate folders)
                }
            );
            var sharePointDuration = context.CurrentUtcDateTime - sharePointStartTime;

            // Add SharePoint sync info to summary
            summary.SharePointSyncEnabled = sharePointSummary.IsEnabled;
            summary.SharePointUploadedCount = sharePointSummary.UploadedCount;

            // Step 7: Log consolidated business metrics
            var uniqueCount = dedupResults.Count(r => !r.IsExactDuplicate && !r.IsSimilar);
            var dupCount = dedupResults.Count - uniqueCount;

            // Convert TimeSpan to seconds for serialization (Durable Functions requirement)
            await context.CallActivityAsync(
                nameof(LogBusinessMetricsActivity),
                new BusinessMetricsRequest
                {
                    BatchId = request.BatchId,
                    SplittingSeconds = splitDuration.TotalSeconds,
                    DeduplicationSeconds = dedupDuration.TotalSeconds,
                    SharePointSeconds = sharePointDuration.TotalSeconds,
                    TotalDocuments = splitResult.Segments?.Count ?? 0,
                    UniqueDocuments = uniqueCount,
                    DuplicateDocuments = dupCount,
                    UploadedToSharePoint = sharePointSummary.UploadedCount
                }
            );

            return summary;
        }
    }

    /// <summary>
    /// Request object for orchestrator.
    /// </summary>
    public class ProcessingRequest
    {
        public string BatchId { get; set; }
        public string BlobName { get; set; }
    }

    /// <summary>
    /// Result from the split activity.
    /// </summary>
    public class SplitResult
    {
        public string? BatchId { get; set; }
        public List<DocumentSegment>? Segments { get; set; }
        public byte[]? OriginalPdfBytes { get; set; }
    }

    /// <summary>
    /// Request for storing results.
    /// </summary>
    public class StoreResultsRequest
    {
        public string? BatchId { get; set; }
        public List<DocumentSegment>? SplitSegments { get; set; }
        public List<DeduplicationResult>? DedupResults { get; set; }
    }

    /// <summary>
    /// Final processing summary returned from the orchestrator.
    /// </summary>
    public class ProcessingSummary
    {
        public string? BatchId { get; set; }
        public int TotalSegments { get; set; }
        public int UniqueSegments { get; set; }
        public int ExactDuplicates { get; set; }
        public int SimilarSegments { get; set; }
        public DateTime ProcessedAt { get; set; }
        public string? Status { get; set; }
        public string? Message { get; set; }
        public bool SharePointSyncEnabled { get; set; }
        public int SharePointUploadedCount { get; set; }
    }
}
---------------using System;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Logs business-focused timing metrics for demo and ROI discussions.
    /// Outputs clean, crisp metrics to Azure Application Insights.
    /// View in Azure Portal or Kudu console: https://docproc-2601282046.scm.azurewebsites.net/DebugConsole
    /// </summary>
    public class BusinessMetricsLogger
    {
        private readonly ILogger<BusinessMetricsLogger> _logger;

        public BusinessMetricsLogger(ILogger<BusinessMetricsLogger> logger)
        {
            _logger = logger;
        }

        public void LogProcessingStart(string batchId, string fileName, int totalPages)
        {
            _logger.LogWarning("========================================");
            _logger.LogWarning("🚀 BUSINESS METRICS - PROCESSING START");
            _logger.LogWarning("========================================");
            _logger.LogWarning("Batch: {BatchId} | File: {FileName} | Pages: {TotalPages}",
                batchId, fileName, totalPages);
            _logger.LogWarning("========================================");
        }

        public void LogPhaseStart(string phaseName)
        {
            _logger.LogWarning("⏱️  {PhaseName} - START", phaseName);
        }

        public void LogPhaseEnd(string phaseName, TimeSpan duration, string result)
        {
            _logger.LogWarning("✅ {PhaseName} - COMPLETE | Duration: {Duration} | Result: {Result}",
                phaseName, FormatDuration(duration), result);
        }

        public void LogProcessingComplete(string batchId, TimeSpan totalDuration, BusinessMetrics metrics)
        {
            var manualTime = EstimateManualProcessingTime(metrics.TotalDocuments);
            var timeSaved = manualTime - totalDuration;
            var percentSaved = (timeSaved.TotalMinutes / manualTime.TotalMinutes) * 100;
            var costSaved = CalculateCostSavings(timeSaved);

            _logger.LogWarning("========================================");
            _logger.LogWarning("🎯 BUSINESS METRICS - PROCESSING COMPLETE");
            _logger.LogWarning("========================================");
            _logger.LogWarning("Batch: {BatchId}", batchId);
            _logger.LogWarning("");
            _logger.LogWarning("⏱️  TIMING BREAKDOWN:");
            _logger.LogWarning("   Splitting:       {SplitTime}", FormatDuration(metrics.SplittingDuration));
            _logger.LogWarning("   Deduplication:   {DedupTime}", FormatDuration(metrics.DeduplicationDuration));
            _logger.LogWarning("   SharePoint:      {SharePointTime}", FormatDuration(metrics.SharePointDuration));
            _logger.LogWarning("   TOTAL:           {TotalTime}", FormatDuration(totalDuration));
            _logger.LogWarning("");
            _logger.LogWarning("📊 RESULTS:");
            _logger.LogWarning("   Total Documents: {Total}", metrics.TotalDocuments);
            _logger.LogWarning("   Unique:          {Unique}", metrics.UniqueDocuments);
            _logger.LogWarning("   Duplicates:      {Duplicates}", metrics.DuplicateDocuments);
            _logger.LogWarning("   Uploaded:        {Uploaded}", metrics.UploadedToSharePoint);
            _logger.LogWarning("");
            _logger.LogWarning("💰 BUSINESS VALUE:");
            _logger.LogWarning("   Automated Time:  {AutoTime}", FormatDuration(totalDuration));
            _logger.LogWarning("   Manual Est:      {ManualTime}", FormatDuration(manualTime));
            _logger.LogWarning("   Time Saved:      {TimeSaved} ({Percent:F0}% reduction)",
                FormatDuration(timeSaved), percentSaved);
            _logger.LogWarning("   Cost Saved:      ${CostSaved:F2} (at $40/hour)", costSaved);
            _logger.LogWarning("========================================");
        }

        public void LogError(string batchId, string phase, string error)
        {
            _logger.LogError("========================================");
            _logger.LogError("❌ BUSINESS METRICS - ERROR");
            _logger.LogError("========================================");
            _logger.LogError("Batch: {BatchId} | Phase: {Phase}", batchId, phase);
            _logger.LogError("Error: {Error}", error);
            _logger.LogError("========================================");
        }

        private string FormatDuration(TimeSpan duration)
        {
            if (duration.TotalHours >= 1)
            {
                return $"{duration.Hours}h {duration.Minutes}m {duration.Seconds}s";
            }
            else if (duration.TotalMinutes >= 1)
            {
                return $"{duration.Minutes}m {duration.Seconds}s";
            }
            else
            {
                return $"{duration.Seconds}s";
            }
        }

        private TimeSpan EstimateManualProcessingTime(int documentCount)
        {
            // Estimate: 2 minutes per document for manual split, classify, organize
            var minutesPerDoc = 2.0;
            var totalMinutes = documentCount * minutesPerDoc;
            return TimeSpan.FromMinutes(totalMinutes);
        }

        private double CalculateCostSavings(TimeSpan timeSaved)
        {
            // At $40/hour labor cost
            var hoursSaved = timeSaved.TotalHours;
            return hoursSaved * 40.0;
        }
    }

    /// <summary>
    /// Business metrics for a processing batch
    /// </summary>
    public class BusinessMetrics
    {
        public TimeSpan SplittingDuration { get; set; }
        public TimeSpan DeduplicationDuration { get; set; }
        public TimeSpan SharePointDuration { get; set; }

        public int TotalDocuments { get; set; }
        public int UniqueDocuments { get; set; }
        public int DuplicateDocuments { get; set; }
        public int UploadedToSharePoint { get; set; }
    }
}
--------------------
using DocumentProcessing.Models;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Service for deduplication logic.
    /// 
    /// Processes hash results and compares against existing index to identify:
    /// - Exact duplicates (same SHA-256 hash).
    /// - Similar documents (low Hamming distance in perceptual hashes).
    /// 
    /// The service works with an in-memory index for the POC. In production,
    /// this would query Azure Table Storage or a database.
    /// </summary>
    public class DeduplicationService
    {
        /// <summary>
        /// Threshold for Hamming distance below which documents are considered "similar".
        /// Hamming distance is the number of differing bits in the perceptual hashes.
        /// A threshold of 5 means up to 5 bits can differ (out of 64 for 8x8 hash) = 7.8% difference.
        /// Very strict to avoid false positives. Use exact SHA-256 matching for true duplicates.
        /// </summary>
        private const int SimilarityThreshold = 5;

        /// <summary>
        /// In-memory index of known documents (segment ID -> HashResult).
        /// In production, this would be loaded from Azure Table Storage.
        /// </summary>
        private readonly Dictionary<string, HashResult> _documentIndex;
        private readonly ILogger<DeduplicationService>? _logger;

        public DeduplicationService(Dictionary<string, HashResult> existingIndex = null, ILogger<DeduplicationService>? logger = null)
        {
            _documentIndex = existingIndex ?? new Dictionary<string, HashResult>();
            _logger = logger;
        }

        /// <summary>
        /// Performs deduplication analysis on a hash result.
        /// Checks against the existing index and returns dedup status.
        /// Also respects pre-detected duplicates from boundary detection.
        /// </summary>
        public DeduplicationResult Deduplicate(HashResult hashResult, List<HashResult>? batchHashResults = null)
        {
            _logger?.LogDebug("Deduplicating segment {SegmentId} (batch: {BatchId}) against index of {IndexCount} entries", 
                hashResult.SegmentId, hashResult.BatchId, _documentIndex.Count);
            var result = new DeduplicationResult
            {
                RecordId = Guid.NewGuid().ToString("N").Substring(0, 12),
                SegmentId = hashResult.SegmentId,
                BlobPath = hashResult.BlobPath,
                Sha256Hash = hashResult.Sha256Hash,
                PerceptualHash = hashResult.PerceptualHash,
                ProcessedAt = DateTime.UtcNow
            };

            // FIRST: Check if this was pre-detected as a within-batch duplicate during boundary detection
            if (hashResult.DetectedAsDuplicate && hashResult.DuplicateOfSegmentIndex.HasValue && batchHashResults != null)
            {
                _logger?.LogInformation("CHECKING pre-detected duplicate: SegmentId={SegId}, DupIndex={DupIdx}, BatchCount={Count}",
                    hashResult.SegmentId, hashResult.DuplicateOfSegmentIndex.Value, batchHashResults.Count);
                
                int origIndex = hashResult.DuplicateOfSegmentIndex.Value;
                if (origIndex >= 0 && origIndex < batchHashResults.Count)
                {
                    var originalSegment = batchHashResults[origIndex];
                    result.IsExactDuplicate = true;
                    result.ExactDuplicateOf = originalSegment.SegmentId;
                    result.Status = "ExactDuplicate";
                    _logger?.LogInformation("Segment {SegmentId}: Pre-detected duplicate of segment {OriginalId} (within-batch)",
                        hashResult.SegmentId, originalSegment.SegmentId);
                    
                    // Still add to index for future batches
                    _documentIndex[hashResult.SegmentId] = hashResult;
                    return result;
                }
            }

            // Check for exact duplicates (same SHA-256) from OTHER batches.
            var exactDuplicate = FindExactDuplicate(hashResult.Sha256Hash, hashResult.BatchId);
            if (exactDuplicate != null)
            {
                result.IsExactDuplicate = true;
                result.ExactDuplicateOf = exactDuplicate.SegmentId;
                result.Status = "ExactDuplicate";
            }
            else
            {
                // Check for similar documents (low Hamming distance) from OTHER batches.
                // Only compare documents with same page count to avoid false positives
                var similarDocs = FindSimilarDocuments(hashResult.PerceptualHash, hashResult.BatchId, hashResult.PageCount);
                if (similarDocs.Count > 0)
                {
                    result.IsSimilar = true;
                    result.SimilarSegmentIds = similarDocs.Select(d => d.hash.SegmentId).ToList();
                    result.MinHammingDistance = similarDocs.Min(d => d.HammingDistance);
                    result.Status = "Similar";
                }
                else
                {
                    result.Status = "Unique";
                }
            }

            // Add this hash result to the index for future comparisons.
            _documentIndex[hashResult.SegmentId] = hashResult;

            return result;
        }

        /// <summary>
        /// Finds an existing document with the same SHA-256 hash.
        /// Only returns duplicates from DIFFERENT batches to avoid false positives.
        /// Skips legacy index entries without BatchId (created before field was added).
        /// </summary>
        private HashResult? FindExactDuplicate(string sha256Hash, string batchId)
        {
            return _documentIndex.Values.FirstOrDefault(h => 
                h.Sha256Hash == sha256Hash && 
                !string.IsNullOrEmpty(h.BatchId) &&  // Skip legacy entries without BatchId
                h.BatchId != batchId);
        }

        /// <summary>
        /// Finds existing documents with similar perceptual hashes (Hamming distance below threshold).
        /// Only returns similar documents from DIFFERENT batches with SAME page count to avoid false positives.
        /// </summary>
        private List<(HashResult hash, int HammingDistance)> FindSimilarDocuments(string perceptualHash, string batchId, int pageCount)
        {
            var similarDocs = new List<(HashResult, int)>();

            foreach (var existingHash in _documentIndex.Values)
            {
                // Skip legacy entries without BatchId (created before field was added)
                if (string.IsNullOrEmpty(existingHash.BatchId))
                {
                    continue;
                }
                
                // Skip comparing segments from the same batch
                if (existingHash.BatchId == batchId)
                {
                    continue;
                }
                
                // Skip if page counts don't match - different documents
                if (existingHash.PageCount != pageCount)
                {
                    continue;
                }
                
                int distance = ComputeHammingDistance(perceptualHash, existingHash.PerceptualHash);
                if (distance <= SimilarityThreshold)
                {
                    similarDocs.Add((existingHash, distance));
                }
            }

            return similarDocs;
        }

        /// <summary>
        /// Computes Hamming distance between two hex-encoded hash strings.
        /// Hamming distance = number of differing bits.
        /// </summary>
        public int ComputeHammingDistance(string hash1, string hash2)
        {
            if (hash1.Length != hash2.Length)
            {
                // Handle different lengths by padding (shouldn't happen with proper hashing).
                var minLength = Math.Min(hash1.Length, hash2.Length);
                hash1 = hash1.Substring(0, minLength);
                hash2 = hash2.Substring(0, minLength);
            }

            // Convert hex strings to bit representation.
            var bits1 = HexStringToBits(hash1);
            var bits2 = HexStringToBits(hash2);

            // Count differing bits.
            int distance = 0;
            for (int i = 0; i < bits1.Length && i < bits2.Length; i++)
            {
                if (bits1[i] != bits2[i])
                {
                    distance++;
                }
            }

            return distance;
        }

        /// <summary>
        /// Converts a hex string to a boolean array representing bits.
        /// </summary>
        private bool[] HexStringToBits(string hexString)
        {
            var bytes = new byte[hexString.Length / 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                bytes[i] = Convert.ToByte(hexString.Substring(i * 2, 2), 16);
            }

            var bits = new bool[bytes.Length * 8];
            for (int i = 0; i < bytes.Length; i++)
            {
                for (int j = 0; j < 8; j++)
                {
                    bits[i * 8 + j] = (bytes[i] & (1 << (7 - j))) != 0;
                }
            }

            return bits;
        }

        /// <summary>
        /// Returns the current document index (for testing/inspection).
        /// </summary>
        public Dictionary<string, HashResult> GetIndex()
        {
            return new Dictionary<string, HashResult>(_documentIndex);
        }
    }
}
------------------using DocumentProcessing.Models;
using iText.Kernel.Pdf;
using iText.Kernel.Pdf.Canvas.Parser;
using Microsoft.Extensions.Logging;
using System.Text.RegularExpressions;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Service for detecting document boundaries using intelligent content analysis.
    /// 
    /// Analyzes PDF text to identify where logical documents begin and end,
    /// returning boundaries with confidence scores and document type classifications.
    /// </summary>
    public class DocumentBoundaryDetector
    {
        public class DetectionConfig
        {
            public double MinBoundaryConfidence { get; set; } = 0.5;
            public bool RequireStrongTitle { get; set; } = false;
            public int MinPagesPerDocument { get; set; } = 1;
            public int MaxTextPreviewLength { get; set; } = 2000;
        }

        private readonly DetectionConfig _config;
        private readonly ILogger<DocumentBoundaryDetector>? _logger;

        // Document type indicators for loan packages and sections
        private static readonly Dictionary<string, string> LoanDocumentIndicators = new()
        {
            // Primary loan documents
            { "closing disclosure", "ClosingDisclosure" },
            { "loan estimate", "LoanEstimate" },
            { "promissory note", "PromissoryNote" },
            { "deed of trust", "DeedOfTrust" },
            { "mortgage", "Mortgage" },
            { "form 1003", "LoanApplication1003" },
            { "uniform residential loan application", "LoanApplication1003" },
            { "appraisal", "AppraisalReport" },
            { "credit report", "CreditReport" },
            { "title insurance policy", "TitlePolicy" },
            { "title policy", "TitlePolicy" },
            { "gift letter", "GiftLetter" },
            { "employment verification", "EmploymentVerification" },
            { "tax return", "TaxReturns" },
            { "bank statement", "BankStatements" },
            { "hud-1", "HUD1Settlement" },
            { "settlement statement", "HUD1Settlement" },
            
            // Top-level section headers (only those that are document boundaries, not sub-sections)
            { "occupancy", "PropertyOccupancy" },
            { "photos", "PropertyPhotos" },
            { "invoice", "InvoiceDocument" }
            
            // REMOVED: Sub-section headers (part of parent documents, not boundaries)
            // { "property type", "PropertyDescription" },      // Sub-section of occupancy
            // { "meter readings", "UtilitiesAnalysis" },       // Sub-section of occupancy
            // { "hazards", "HazardsAnalysis" },                // Sub-section of occupancy
            // { "utilities", "UtilitiesAnalysis" }             // Sub-section of occupancy
        };

        public DocumentBoundaryDetector(DetectionConfig? config = null, ILogger<DocumentBoundaryDetector>? logger = null)
        {
            _config = config ?? new DetectionConfig();
            _logger = logger;
        }

        /// <summary>
        /// Detects document boundaries in a PDF using text extraction and content analysis.
        /// </summary>
        public List<DocumentBoundary> DetectBoundaries(byte[] pdfBytes)
        {
            if (pdfBytes == null || pdfBytes.Length == 0)
            {
                _logger?.LogWarning("DetectBoundaries called with null or empty PDF bytes");
                return new List<DocumentBoundary>();
            }

            try
            {
                _logger?.LogInformation("DetectBoundaries: Starting boundary detection for {ByteCount} bytes", pdfBytes.Length);
                
                var pageTexts = ExtractPagesFromPdf(pdfBytes);
                _logger?.LogInformation("DetectBoundaries: Extracted {PageCount} pages from PDF", pageTexts.Count);
                
                if (pageTexts.Count == 0)
                {
                    _logger?.LogError("DetectBoundaries: No pages extracted from PDF - cannot detect boundaries");
                    return new List<DocumentBoundary>();
                }

                // Log text extraction results
                int totalChars = pageTexts.Sum(p => p.Length);
                int emptyPages = pageTexts.Count(p => p.Length == 0);
                _logger?.LogInformation("DetectBoundaries: Text stats - {TotalChars} total chars, {EmptyPages}/{PageCount} empty pages", 
                    totalChars, emptyPages, pageTexts.Count);

                if (totalChars == 0)
                {
                    _logger?.LogCritical("DetectBoundaries: ZERO characters extracted from all pages. PDF is scanned/image-based, encrypted, or has unsupported fonts. Cannot detect boundaries without OCR or PDF remediation. Falling back to single document.");
                    return new List<DocumentBoundary>();
                }

                var boundaries = new List<DocumentBoundary>();
                string? previousDocType = null;

                for (int i = 0; i < pageTexts.Count; i++)
                {
                    var boundary = AnalyzeBoundary(
                        pageTexts[i],
                        i > 0 ? pageTexts[i - 1] : null,
                        i
                    );

                    if (boundary != null && boundary.ConfidenceScore >= _config.MinBoundaryConfidence)
                    {
                        // KEY FIX: Only report boundary if document type CHANGED
                        // Don't report boundaries for consecutive pages of the same document
                        if (previousDocType != boundary.DocumentType)
                        {
                            _logger?.LogInformation("DetectBoundaries: Boundary detected at page {PageNum}: {DocType} (confidence: {Confidence:P1}) - Type changed from '{PrevType}'", 
                                i, boundary.DocumentType, boundary.ConfidenceScore, previousDocType ?? "START");
                            boundaries.Add(boundary);
                        }
                        else
                        {
                            _logger?.LogDebug("Page {PageNum}: Same document type '{DocType}' - not reporting as boundary", 
                                i, boundary.DocumentType);
                        }
                        
                        // Update previous doc type (whether or not we reported the boundary)
                        previousDocType = boundary.DocumentType;
                    }
                    else if (boundary != null)
                    {
                        // Page analyzed but below confidence threshold
                        _logger?.LogDebug("Page {PageNum}: Boundary confidence {Confidence:P1} below threshold {Threshold:P1}", 
                            i, boundary.ConfidenceScore, _config.MinBoundaryConfidence);
                        previousDocType = boundary.DocumentType;
                    }
                }

                _logger?.LogInformation("DetectBoundaries: Complete - {BoundaryCount} boundaries detected (threshold: {MinConfidence:P1})", 
                    boundaries.Count, _config.MinBoundaryConfidence);
                
                return boundaries;
            }
            catch (Exception ex)
            {
                // Graceful degradation: return empty list on extraction failure
                _logger?.LogError(ex, "DetectBoundaries: Exception during boundary detection: {Message}", ex.Message);
                return new List<DocumentBoundary>();
            }
        }

        /// <summary>
        /// Extracts text from each page of a PDF.
        /// </summary>
        private List<PageText> ExtractPagesFromPdf(byte[] pdfBytes)
        {
            var pages = new List<PageText>();

            try
            {
                using (var reader = new PdfReader(new MemoryStream(pdfBytes)))
                using (var document = new PdfDocument(reader))
                {
                    int pageCount = document.GetNumberOfPages();
                    _logger?.LogInformation("PDF has {PageCount} pages", pageCount);

                    // Check if PDF is encrypted
                    if (reader.IsEncrypted())
                    {
                        _logger?.LogWarning("PDF is encrypted. Text extraction may fail or return empty strings. Check if extraction permissions are enabled.");
                    }

                    for (int pageNum = 1; pageNum <= pageCount; pageNum++)
                    {
                        try
                        {
                            var page = document.GetPage(pageNum);
                            
                            // Try text extraction - iText7 should handle text-based PDFs
                            // Using default strategy which may not work for all PDFs
                            string text = PdfTextExtractor.GetTextFromPage(page);

                            // DIAGNOSTIC: If text is empty but PDF is readable, try alternative extraction
                            if (string.IsNullOrWhiteSpace(text))
                            {
                                _logger?.LogDebug("Page {PageNum}: Standard extraction returned empty. Attempting alternative extraction methods...", pageNum);
                                
                                // Note: Alternative extraction strategies require additional iText7 modules
                                // For now, accept empty result and log it
                                _logger?.LogDebug("Page {PageNum}: Alternative extraction not available in current configuration", pageNum);
                            }

                            // Log extraction results with diagnostic info
                            int textLength = text?.Length ?? 0;
                            string textPreview = string.IsNullOrWhiteSpace(text) 
                                ? "[EMPTY - PDF may be scanned/image-based or encrypted]" 
                                : text.Substring(0, Math.Min(100, textLength)).Replace("\n", "\\n");
                            
                            _logger?.LogDebug("Page {PageNum}: Extracted {TextLength} chars - Preview: {Preview}", 
                                pageNum, textLength, textPreview);

                            // If no text extracted, log a warning (PDF might be scanned or encrypted)
                            if (string.IsNullOrWhiteSpace(text))
                            {
                                _logger?.LogWarning("Page {PageNum}: Text extraction returned empty/whitespace. PDF may be image-based (scanned), encrypted, or have unsupported fonts.", pageNum);
                            }

                            pages.Add(new PageText
                            {
                                PageNumber = pageNum - 1,  // 0-indexed
                                Content = text ?? string.Empty,
                                Length = textLength
                            });
                        }
                        catch (Exception ex)
                        {
                            // If extraction fails for specific page, add empty page and log warning
                            _logger?.LogWarning(ex, "Failed to extract text from page {PageNum}. Error: {Message}", pageNum, ex.Message);
                            pages.Add(new PageText
                            {
                                PageNumber = pageNum - 1,
                                Content = string.Empty,
                                Length = 0
                            });
                        }
                    }

                    // Summary logging
                    int totalChars = pages.Sum(p => p.Length);
                    int emptyPages = pages.Count(p => p.Length == 0);
                    _logger?.LogInformation("Text extraction complete: {PageCount} pages, {TotalChars} total characters, {EmptyPages} empty pages", 
                        pages.Count, totalChars, emptyPages);

                    if (emptyPages == pageCount)
                    {
                        _logger?.LogError("CRITICAL: ALL pages returned empty text. PDF is likely scanned/image-based, encrypted, or has unsupported fonts. Boundary detection cannot proceed without OCR or PDF remediation.");
                    }
                }
            }
            catch (Exception ex)
            {
                // If PDF reading fails entirely, return empty list
                _logger?.LogError(ex, "Failed to read PDF. Error: {Message}", ex.Message);
                return new List<PageText>();
            }

            return pages;
        }

        /// <summary>
        /// Analyzes a page to detect if it's a document boundary.
        /// </summary>
        private DocumentBoundary? AnalyzeBoundary(PageText currentPage, PageText? previousPage, int pageNumber)
        {
            if (string.IsNullOrWhiteSpace(currentPage.Content))
            {
                _logger?.LogDebug("Page {PageNum} has no text content - skipping boundary analysis", pageNumber);
                return null;
            }

            var boundarySignals = new List<BoundarySignal>();
            double totalWeight = 0;
            double supportingWeight = 0;

            // Extract entities from current page
            var entities = ExtractPageEntities(currentPage.Content);

            // Check for document title (strong indicator)
            var titleMatch = DetectDocumentTitle(currentPage.Content);
            if (titleMatch != null)
            {
                _logger?.LogDebug("Page {PageNum}: Detected document title '{DocType}'", pageNumber, titleMatch.Value.DocType);
                boundarySignals.Add(new BoundarySignal
                {
                    SignalType = BoundarySignalType.StrongTitle,
                    Description = titleMatch.Value.DocType,
                    Weight = 0.9,
                    SupportsNewBoundary = true
                });
                supportingWeight += 0.9;
            }

            // Check for form references
            if (DetectFormReference(currentPage.Content))
            {
                _logger?.LogDebug("Page {PageNum}: Detected form reference", pageNumber);
                boundarySignals.Add(new BoundarySignal
                {
                    SignalType = BoundarySignalType.FormReference,
                    Description = "Known form reference detected",
                    Weight = 0.8,
                    SupportsNewBoundary = true
                });
                supportingWeight += 0.8;
            }

            // Check for signature page
            if (DetectSignaturePage(currentPage.Content))
            {
                _logger?.LogDebug("Page {PageNum}: Detected signature page", pageNumber);
                boundarySignals.Add(new BoundarySignal
                {
                    SignalType = BoundarySignalType.SignaturePage,
                    Description = "Signature page detected",
                    Weight = 0.75,
                    SupportsNewBoundary = true
                });
                supportingWeight += 0.75;
            }

            // Check for entity changes (borrower, loan number, address) - HIGH PRIORITY
            // These are STRONGEST signals that we've moved to a new document
            if (previousPage != null && !string.IsNullOrWhiteSpace(previousPage.Content))
            {
                var previousEntities = ExtractPageEntities(previousPage.Content);
                double entityChangeScore = 0.0;
                int entitySignalsFound = 0;
                
                // SIGNAL: Borrower name changed (highest priority)
                if (previousEntities.BorrowerNames.Count > 0 && entities.BorrowerNames.Count > 0)
                {
                    var prevBorrowers = string.Join("|", previousEntities.BorrowerNames).ToLower();
                    var currBorrowers = string.Join("|", entities.BorrowerNames).ToLower();
                    
                    if (prevBorrowers != currBorrowers)
                    {
                        _logger?.LogInformation("Page {PageNum}: Entity boundary - BORROWER CHANGED from '{Prev}' to '{Curr}'", 
                            pageNumber, prevBorrowers, currBorrowers);
                        entityChangeScore += 0.85;  // Very strong signal
                        entitySignalsFound++;
                    }
                }
                
                // SIGNAL: Loan number changed
                if (!string.IsNullOrEmpty(previousEntities.LoanNumber) && !string.IsNullOrEmpty(entities.LoanNumber))
                {
                    if (previousEntities.LoanNumber != entities.LoanNumber)
                    {
                        _logger?.LogInformation("Page {PageNum}: Entity boundary - LOAN NUMBER CHANGED from '{Prev}' to '{Curr}'",
                            pageNumber, previousEntities.LoanNumber, entities.LoanNumber);
                        entityChangeScore += 0.80;  // Very strong signal
                        entitySignalsFound++;
                    }
                }
                
                // SIGNAL: Property address changed
                if (!string.IsNullOrEmpty(previousEntities.PropertyAddress) && !string.IsNullOrEmpty(entities.PropertyAddress))
                {
                    if (previousEntities.PropertyAddress != entities.PropertyAddress)
                    {
                        _logger?.LogInformation("Page {PageNum}: Entity boundary - PROPERTY ADDRESS CHANGED",
                            pageNumber);
                        entityChangeScore += 0.75;  // Strong signal
                        entitySignalsFound++;
                    }
                }
                
                // Add entity change signal if any found
                if (entitySignalsFound > 0)
                {
                    boundarySignals.Add(new BoundarySignal
                    {
                        SignalType = BoundarySignalType.EntityChange,
                        Description = $"Entity changed ({entitySignalsFound} signal{(entitySignalsFound > 1 ? "s" : "")})",
                        Weight = entityChangeScore,
                        SupportsNewBoundary = true
                    });
                    supportingWeight += entityChangeScore;
                    totalWeight += entityChangeScore;
                }
            }

            // Check for continuation markers (opposes boundary, but with lower weight)
            // Reduced from 0.6 to 0.3 because page headers like "Page X of Y" are less reliable
            if (DetectContinuationMarker(currentPage.Content))
            {
                _logger?.LogDebug("Page {PageNum}: Detected continuation marker - weak opposition to boundary", pageNumber);
                boundarySignals.Add(new BoundarySignal
                {
                    SignalType = BoundarySignalType.ContinuationMarker,
                    Description = "Continuation of previous document",
                    Weight = 0.3,  // REDUCED: Page headers are less reliable
                    SupportsNewBoundary = false
                });
                supportingWeight -= 0.3;
            }

            // Check for explicit delimiters
            if (DetectExplicitDelimiter(currentPage.Content))
            {
                _logger?.LogDebug("Page {PageNum}: Detected explicit document delimiter", pageNumber);
                boundarySignals.Add(new BoundarySignal
                {
                    SignalType = BoundarySignalType.ExplicitDelimiter,
                    Description = "Explicit document delimiter found",
                    Weight = 1.0,
                    SupportsNewBoundary = true
                });
                supportingWeight += 1.0;
            }

            totalWeight = Math.Abs(supportingWeight);
            if (totalWeight == 0)
            {
                _logger?.LogDebug("Page {PageNum}: No boundary signals detected", pageNumber);
                return null;
            }

            double confidenceScore = supportingWeight / totalWeight;

            // Determine document type
            var docType = titleMatch?.DocType ?? "Unknown";
            var docTypeConfidence = titleMatch?.Confidence ?? 0.0;

            var boundary = new DocumentBoundary
            {
                PageNumber = pageNumber,
                ConfidenceScore = Math.Max(0, Math.Min(1, confidenceScore)), // Clamp 0-1
                DocumentType = docType,
                DocumentTypeConfidence = docTypeConfidence,
                BoundarySignals = boundarySignals,
                PageEntities = entities
            };

            _logger?.LogDebug("Page {PageNum}: Boundary analysis complete - Confidence: {Confidence:P1}, Signals: {SignalCount}, DocType: {DocType}",
                pageNumber, boundary.ConfidenceScore, boundarySignals.Count, docType);

            return boundary;
        }

        /// <summary>
        /// Detects if the page contains a document title or section header.
        /// Enhanced to scan entire page, not just first 10 lines.
        /// </summary>
        private (string DocType, double Confidence)? DetectDocumentTitle(string pageText)
        {
            var allLines = pageText.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(l => l.Trim())
                .Where(l => l.Length > 3)
                .ToList();

            // First priority: Check first 10 lines (header/title area)
            foreach (var line in allLines.Take(10))
            {
                foreach (var (indicator, docType) in LoanDocumentIndicators)
                {
                    if (line.Contains(indicator, StringComparison.OrdinalIgnoreCase))
                    {
                        _logger?.LogDebug("DetectDocumentTitle: Found '{Indicator}' in first 10 lines → {DocType}", indicator, docType);
                        return (docType, 0.95);  // Higher confidence for header-area detection
                    }
                }
            }

            // Secondary: Check remaining lines (may be section headers)
            foreach (var line in allLines.Skip(10))
            {
                foreach (var (indicator, docType) in LoanDocumentIndicators)
                {
                    if (line.Contains(indicator, StringComparison.OrdinalIgnoreCase))
                    {
                        _logger?.LogDebug("DetectDocumentTitle: Found '{Indicator}' in page body → {DocType}", indicator, docType);
                        return (docType, 0.85);  // Slightly lower confidence for mid-page headers
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Detects form references (e.g., "Form 1003", "HUD-1").
        /// </summary>
        private bool DetectFormReference(string pageText)
        {
            var formPattern = new Regex(@"(?:Form|FORM)\s+(\d+|HUD[- ]?\d)", RegexOptions.IgnoreCase);
            return formPattern.IsMatch(pageText);
        }

        /// <summary>
        /// Detects if the page is a signature page.
        /// </summary>
        private bool DetectSignaturePage(string pageText)
        {
            var signaturePatterns = new[]
            {
                @"\bSignature\b",
                @"\bInitial\b",
                @"\bSigned\b",
                @"\bby\s+(?:and\s+)?between\b",
                @"Date\s*[_:]"
            };

            int signatureCount = signaturePatterns.Count(p =>
                Regex.IsMatch(pageText, p, RegexOptions.IgnoreCase)
            );

            return signatureCount >= 2;  // Multiple signature indicators
        }

        /// <summary>
        /// Detects continuation markers that indicate document continuation.
        /// </summary>
        private bool DetectContinuationMarker(string pageText)
        {
            var continuationPatterns = new[]
            {
                @"page\s+\d+\s+of\s+\d+",
                @"continued",
                @"continued on next page",
                @"page\s+break"
            };

            return continuationPatterns.Any(p =>
                Regex.IsMatch(pageText, p, RegexOptions.IgnoreCase)
            );
        }

        /// <summary>
        /// Detects explicit document delimiters.
        /// </summary>
        private bool DetectExplicitDelimiter(string pageText)
        {
            var delimiterPatterns = new[]
            {
                @"end of document",
                @"---+",
                @"===+",
                @"\*{5,}"
            };

            return delimiterPatterns.Any(p =>
                Regex.IsMatch(pageText, p, RegexOptions.IgnoreCase)
            );
        }

        /// <summary>
        /// Extracts key entities (borrower names, loan numbers, addresses) from page text.
        /// </summary>
        private PageEntities ExtractPageEntities(string pageText)
        {
            var entities = new PageEntities();

            // Extract borrower names
            var borrowerPattern = new Regex(@"Borrower(?:s)?:?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", RegexOptions.IgnoreCase);
            var borrowerMatches = borrowerPattern.Matches(pageText);
            entities.BorrowerNames = borrowerMatches.Cast<Match>()
                .Select(m => m.Groups[1].Value)
                .Distinct()
                .ToList();
            
            if (entities.BorrowerNames.Count > 0)
                _logger?.LogDebug("Extracted borrower names: {Names}", string.Join(", ", entities.BorrowerNames));

            // Extract loan number
            var loanNumberPattern = new Regex(@"(?:Loan|Loan\s+#|Loan\s+Number)[\s:]*([A-Za-z0-9\-]{6,20})", RegexOptions.IgnoreCase);
            var loanMatch = loanNumberPattern.Match(pageText);
            if (loanMatch.Success)
            {
                entities.LoanNumber = loanMatch.Groups[1].Value;
                _logger?.LogDebug("Extracted loan number: {LoanNumber}", entities.LoanNumber);
            }

            // Extract property address
            var addressPattern = new Regex(@"(?:Property|Address)[\s:]*([0-9]+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln)[\s,]*[\w\s]*)", RegexOptions.IgnoreCase);
            var addressMatch = addressPattern.Match(pageText);
            if (addressMatch.Success)
            {
                entities.PropertyAddress = addressMatch.Groups[1].Value;
                _logger?.LogDebug("Extracted property address: {Address}", entities.PropertyAddress);
            }

            // Extract form references
            var formPattern = new Regex(@"(?:Form|FORM)\s+(\d+)", RegexOptions.IgnoreCase);
            var formMatches = formPattern.Matches(pageText);
            entities.FormReferences = formMatches.Cast<Match>()
                .Select(m => m.Groups[1].Value)
                .Distinct()
                .ToList();
            
            if (entities.FormReferences.Count > 0)
                _logger?.LogDebug("Extracted form references: {Forms}", string.Join(", ", entities.FormReferences));

            return entities;
        }


        /// <summary>
        /// Internal model for extracted page text.
        /// </summary>
        private class PageText
        {
            public int PageNumber { get; set; }
            public string Content { get; set; } = string.Empty;
            public int Length { get; set; }
        }
    }
}
-------------
using Azure;
using Azure.AI.FormRecognizer.DocumentAnalysis;
using Microsoft.Extensions.Logging;
using System.Text;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Service for analyzing PDF documents using Azure AI Document Intelligence.
    /// Extracts layout, structure, headings, paragraphs, and tables for intelligent splitting.
    /// </summary>
    public class DocumentIntelligenceService
    {
        private readonly DocumentAnalysisClient _client;
        private readonly ILogger<DocumentIntelligenceService>? _logger;

        public DocumentIntelligenceService(string endpoint, string apiKey, ILogger<DocumentIntelligenceService>? logger = null)
        {
            _client = new DocumentAnalysisClient(new Uri(endpoint), new AzureKeyCredential(apiKey));
            _logger = logger;
        }

        /// <summary>
        /// Analyzes a PDF document and returns structured content with layout information.
        /// Uses the prebuilt "layout" model which extracts text, tables, and structure.
        /// </summary>
        public async Task<DocumentIntelligenceResult> AnalyzeDocumentAsync(byte[] pdfBytes, string fileName)
        {
            try
            {
                _logger?.LogInformation("Starting Document Intelligence analysis for {FileName} ({ByteCount} bytes)", 
                    fileName, pdfBytes.Length);

                // Use the prebuilt "layout" model for structure extraction
                using var stream = new MemoryStream(pdfBytes);
                
                var operation = await _client.AnalyzeDocumentAsync(
                    WaitUntil.Completed, 
                    "prebuilt-layout", 
                    stream
                );

                var result = operation.Value;

                _logger?.LogInformation("Analysis complete: {PageCount} pages, {ParagraphCount} paragraphs, {TableCount} tables",
                    result.Pages.Count, result.Paragraphs.Count, result.Tables.Count);

                return ConvertToInternalModel(result, fileName);
            }
            catch (RequestFailedException ex)
            {
                _logger?.LogError(ex, "Document Intelligence API error: {StatusCode} - {Message}", 
                    ex.Status, ex.Message);
                throw new InvalidOperationException($"Document Intelligence analysis failed: {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Unexpected error during Document Intelligence analysis: {Message}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Converts Document Intelligence SDK result to our internal model.
        /// </summary>
        private DocumentIntelligenceResult ConvertToInternalModel(AnalyzeResult result, string fileName)
        {
            var internalResult = new DocumentIntelligenceResult
            {
                FileName = fileName,
                PageCount = result.Pages.Count,
                Pages = new List<PageAnalysis>()
            };

            // Process each page
            for (int pageNum = 0; pageNum < result.Pages.Count; pageNum++)
            {
                var page = result.Pages[pageNum];
                var pageAnalysis = new PageAnalysis
                {
                    PageNumber = pageNum,
                    Width = page.Width ?? 0,
                    Height = page.Height ?? 0,
                    Unit = page.Unit?.ToString() ?? "inch",
                    Paragraphs = new List<ParagraphInfo>(),
                    Tables = new List<TableInfo>(),
                    PageHeader = null,
                    PageFooter = null,
                    PageNumberText = null
                };

                // Extract paragraphs on this page (SDK uses 1-indexed pages)
                var pageParagraphs = result.Paragraphs
                    .Where(p => p.BoundingRegions != null && 
                                p.BoundingRegions.Any(br => br.PageNumber == pageNum + 1))
                    .ToList();

                foreach (var para in pageParagraphs)
                {
                    var firstBoundingRegion = para.BoundingRegions?.FirstOrDefault();
                    var roleString = para.Role.HasValue ? para.Role.Value.ToString() : null;
                    
                    var paraInfo = new ParagraphInfo
                    {
                        Content = para.Content,
                        Role = roleString,
                        BoundingBox = firstBoundingRegion?.BoundingPolygon?.Select(p => 
                            new System.Drawing.PointF(p.X, p.Y)).ToList()
                    };
                    
                    pageAnalysis.Paragraphs.Add(paraInfo);
                    
                    // Capture page-level metadata from paragraph roles
                    if (roleString == "pageHeader" && pageAnalysis.PageHeader == null)
                    {
                        pageAnalysis.PageHeader = para.Content;
                    }
                    else if (roleString == "pageFooter" && pageAnalysis.PageFooter == null)
                    {
                        pageAnalysis.PageFooter = para.Content;
                    }
                    else if (roleString == "pageNumber" && pageAnalysis.PageNumberText == null)
                    {
                        pageAnalysis.PageNumberText = para.Content;
                    }
                }

                // Extract tables on this page
                var pageTables = result.Tables
                    .Where(t => t.BoundingRegions != null && 
                                t.BoundingRegions.Any(br => br.PageNumber == pageNum + 1))
                    .ToList();

                foreach (var table in pageTables)
                {
                    pageAnalysis.Tables.Add(new TableInfo
                    {
                        RowCount = table.RowCount,
                        ColumnCount = table.ColumnCount,
                        Cells = table.Cells.Select(c => new TableCell
                        {
                            RowIndex = c.RowIndex,
                            ColumnIndex = c.ColumnIndex,
                            Content = c.Content,
                            IsHeader = c.Kind == DocumentTableCellKind.ColumnHeader || 
                                      c.Kind == DocumentTableCellKind.RowHeader
                        }).ToList()
                    });
                }

                internalResult.Pages.Add(pageAnalysis);
            }

            return internalResult;
        }

        /// <summary>
        /// Extracts full text content from the analysis result in page order.
        /// </summary>
        public string ExtractFullText(DocumentIntelligenceResult result)
        {
            var sb = new StringBuilder();
            
            foreach (var page in result.Pages)
            {
                sb.AppendLine($"--- Page {page.PageNumber + 1} ---");
                
                foreach (var para in page.Paragraphs)
                {
                    sb.AppendLine(para.Content);
                }
                
                sb.AppendLine();
            }
            
            return sb.ToString();
        }
    }

    /// <summary>
    /// Internal model for Document Intelligence analysis results.
    /// Simplified from Azure SDK model for easier consumption.
    /// </summary>
    public class DocumentIntelligenceResult
    {
        public string FileName { get; set; } = string.Empty;
        public int PageCount { get; set; }
        public List<PageAnalysis> Pages { get; set; } = new();
    }

    public class PageAnalysis
    {
        public int PageNumber { get; set; }
        public double Width { get; set; }
        public double Height { get; set; }
        public string Unit { get; set; } = "inch";
        public List<ParagraphInfo> Paragraphs { get; set; } = new();
        public List<TableInfo> Tables { get; set; } = new();
        public string? PageHeader { get; set; }
        public string? PageFooter { get; set; }
        public string? PageNumberText { get; set; }
    }

    public class ParagraphInfo
    {
        public string Content { get; set; } = string.Empty;
        public string? Role { get; set; } // "title", "sectionHeading", "pageHeader", "pageFooter", etc.
        public List<System.Drawing.PointF>? BoundingBox { get; set; }
    }

    public class TableInfo
    {
        public int RowCount { get; set; }
        public int ColumnCount { get; set; }
        public List<TableCell> Cells { get; set; } = new();
    }

    public class TableCell
    {
        public int RowIndex { get; set; }
        public int ColumnIndex { get; set; }
        public string Content { get; set; } = string.Empty;
        public bool IsHeader { get; set; }
    }
}
-----------------------------
using DocumentProcessing.Models;
using System.IO;
using System.Text.RegularExpressions;
using iText.Kernel.Pdf;
using iText.Kernel.Utils;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Service for splitting PDF documents on LOGICAL DOCUMENT BOUNDARIES.
    /// 
    /// REQUIREMENT: Split incoming PDFs into logical documents (not individual pages).
    /// Example: 18-page PDF containing [Closing Disclosure (5p) + Loan Estimate (3p) + Appraisal (10p)]
    ///          Should result in 3 DocumentSegments, not 18.
    /// 
    /// STRATEGY:
    /// 1. Call DocumentBoundaryDetector to analyze PDF and find logical document boundaries
    /// 2. Detector returns list of PageNumbers where logical documents begin
    /// 3. Calculate page ranges between boundaries
    /// 4. Use ExtractPageRange() with iText7.CopyPagesTo() to extract each range
    /// 5. Return DocumentSegments with type and confidence info from detector
    /// 
    /// GRACEFUL FALLBACK: If text extraction disabled, entire PDF treated as single document.
    /// </summary>
    public class DocumentSplitterService
    {
        private readonly DocumentBoundaryDetector? _boundaryDetector;
        private readonly ILogger<DocumentSplitterService> _logger;
        
        /// <summary>
        /// Minimum character count for a page to be considered non-blank.
        /// Pages with fewer characters are treated as blank and excluded from segments.
        /// Increased to 100 to avoid false positives on pages with minimal formatting.
        /// </summary>
        private const int MinCharactersForNonBlankPage = 100;
        
        /// <summary>
        /// Feature flag to enable blank page removal.
        /// Set to false to keep all pages including blank ones.
        /// Uses Document Intelligence paragraph/table counts for detection (works with image-based PDFs).
        /// </summary>
        private const bool EnableBlankPageRemoval = true;  // ENABLED - uses Document Intelligence

        public DocumentSplitterService(DocumentBoundaryDetector? boundaryDetector = null, ILogger<DocumentSplitterService>? logger = null)
        {
            _boundaryDetector = boundaryDetector;
            _logger = logger ?? new NullLogger<DocumentSplitterService>();
        }

        /// <summary>
        /// TEST METHOD: Verify iText7 is properly integrated and can read PDF information.
        /// This method validates that iText7 namespaces are accessible and functional.
        /// </summary>
        public string VerifyIText7Integration(byte[] pdfBytes)
        {
            try
            {
                // Use iText7 to read PDF information
                using (var memoryStream = new MemoryStream(pdfBytes))
                {
                    using (var pdfReader = new PdfReader(memoryStream))
                    {
                        using (var pdfDocument = new PdfDocument(pdfReader))
                        {
                            int pageCount = pdfDocument.GetNumberOfPages();
                            string pdfVersion = pdfDocument.GetPdfVersion().ToString();
                            
                            return $"✅ iText7 Integration SUCCESS!\n" +
                                   $"   - PDF Version: {pdfVersion}\n" +
                                   $"   - Page Count: {pageCount}\n" +
                                   $"   - iText7 Namespace: iText.Kernel.Pdf is accessible\n" +
                                   $"   - Ready for page extraction implementation";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return $"iText7 Integration FAILED:\n" +
                       $"   - Error: {ex.Message}\n" +
                       $"   - Type: {ex.GetType().Name}";
            }
        }

        /// <summary>
        /// Splits a PDF into logical documents based on detected boundaries.
        /// 
        /// FLOW:
        /// 1. Load PDF and call DocumentBoundaryDetector.DetectBoundaries()
        /// 2. Detector returns list of boundaries with page numbers and confidence scores
        /// 3. Calculate logical document ranges from detected boundaries
        /// 4. Use ExtractPageRange() to copy each range into a new PDF
        /// 5. Create DocumentSegment for each logical document
        /// 
        /// FALLBACK: If no boundaries detected, entire PDF becomes single segment.
        /// </summary>
        public List<DocumentSegment> SplitPdf(byte[] pdfBytes, string sourceFileName)
        {
            var segments = new List<DocumentSegment>();

            try
            {
                var fileNameBase = SanitizeFileName(Path.GetFileNameWithoutExtension(sourceFileName));

                // Step 1: Load the source PDF
                using (var sourceStream = new MemoryStream(pdfBytes))
                using (var pdfReader = new PdfReader(sourceStream))
                using (var sourcePdfDocument = new PdfDocument(pdfReader))
                {
                    int totalPages = sourcePdfDocument.GetNumberOfPages();

                    // Step 2: Detect logical document boundaries in the PDF (with duplicate detection)
                    List<DocumentBoundary> boundaries = new List<DocumentBoundary>();
                    if (_boundaryDetector != null)
                    {
                        _logger.LogDebug("Detecting document boundaries in {SourceFile} ({PageCount} pages)", sourceFileName, totalPages);
                        boundaries = _boundaryDetector.DetectBoundaries(pdfBytes);
                        _logger.LogInformation("Boundary detection complete: {BoundaryCount} boundaries found (includes duplicate detection)", boundaries.Count);
                    }
                    else
                    {
                        _logger.LogWarning("DocumentBoundaryDetector not injected - treating entire PDF as single document");
                    }

                    // Step 3: Determine split points based on detected boundaries
                    List<(int startPage, int endPage, string docType, double confidence)> documentRanges;
                    
                    if (boundaries.Count > 0)
                    {
                        // Split at detected boundaries
                        _logger.LogDebug("Calculating document ranges from {BoundaryCount} detected boundaries", boundaries.Count);
                        documentRanges = CalculateDocumentRanges(boundaries, totalPages);
                        _logger.LogInformation("Will split into {RangeCount} logical documents", documentRanges.Count);
                    }
                    else
                    {
                        // No boundaries detected - treat entire PDF as single document
                        _logger.LogInformation("No boundaries detected - treating entire PDF as single document");
                        documentRanges = new List<(int, int, string, double)>
                        {
                            (0, totalPages - 1, LoanDocumentTypes.Unknown, 0.0)
                        };
                    }

                    // Step 4: Extract each logical document using iText7.CopyPagesTo()
                    int segmentNumber = 1;
                    int segmentIndex = 0;  // Track segment index for duplicate reference
                    
                    // Build a map of start page -> segment index for duplicate lookup
                    var startPageToSegmentIndex = new Dictionary<int, int>();
                    
                    foreach (var (startPage, endPage, docType, docConfidence) in documentRanges)
                    {
                        try
                        {
                            _logger.LogDebug("Extracting segment {SegmentNum}: pages {StartPage}-{EndPage}", 
                                segmentNumber, startPage + 1, endPage + 1);
                            byte[] logicalDocBytes = ExtractPageRange(pdfBytes, startPage, endPage);

                            if (logicalDocBytes.Length > 0)
                            {
                                // Find the boundary for this segment's start page
                                var matchingBoundary = boundaries.FirstOrDefault(b => b.PageNumber == startPage);
                                bool isDuplicate = matchingBoundary?.IsDuplicateBoundary ?? false;
                                int? duplicateOfIndex = null;
                                
                                _logger.LogInformation("Segment {SegNum} check: startPage={Start}, boundaryFound={Found}, isDup={IsDup}",
                                    segmentNumber, startPage, matchingBoundary != null, isDuplicate);
                                
                                // If this is a duplicate, find the segment index it duplicates
                                if (isDuplicate && matchingBoundary?.DuplicateOfPageNumber.HasValue == true)
                                {
                                    int originalPage = matchingBoundary.DuplicateOfPageNumber.Value;
                                    if (startPageToSegmentIndex.TryGetValue(originalPage, out int origIndex))
                                    {
                                        duplicateOfIndex = origIndex;
                                        _logger.LogInformation("Segment {SegmentNum} (pages {StartPage}-{EndPage}) marked as duplicate of segment {OrigIndex} (page {OrigPage})",
                                            segmentNumber, startPage + 1, endPage + 1, origIndex + 1, originalPage + 1);
                                    }
                                }
                                
                                var segment = new DocumentSegment
                                {
                                    SegmentId = Guid.NewGuid().ToString("N").Substring(0, 12),
                                    SourceFileName = sourceFileName,
                                    StartPageNumber = startPage + 1,  // 1-indexed for display
                                    EndPageNumber = endPage + 1,      // 1-indexed for display
                                    PageCount = (endPage - startPage) + 1,
                                    OutputBlobPath = $"split-documents/{fileNameBase}/segment-{segmentNumber:D3}.pdf",
                                    SizeBytes = logicalDocBytes.Length,
                                    CreatedAt = DateTime.UtcNow,
                                    SegmentBytes = logicalDocBytes,
                                    DocumentType = docType,
                                    DocumentTypeConfidence = docConfidence,
                                    BoundaryConfidence = matchingBoundary?.ConfidenceScore ?? 0.0,
                                    DetectedAsDuplicate = isDuplicate,
                                    DuplicateOfSegmentIndex = duplicateOfIndex
                                };

                                // Track this segment's start page for future duplicate references
                                startPageToSegmentIndex[startPage] = segmentIndex;
                                
                                segments.Add(segment);
                                segmentNumber++;
                                segmentIndex++;
                            }
                            else
                            {
                                _logger.LogWarning("Segment {SegmentNum} (pages {StartPage}-{EndPage}) is empty after removing blank pages - skipping",
                                    segmentNumber, startPage + 1, endPage + 1);
                            }
                        }
                        catch (Exception segmentEx)
                        {
                            _logger.LogError(segmentEx, "Failed to extract segment for pages {StartPage}-{EndPage} from {SourceFile}. Error: {Message}",
                                startPage + 1, endPage + 1, sourceFileName, segmentEx.Message);
                            throw new InvalidOperationException(
                                $"Segment extraction failed for pages {startPage + 1}-{endPage + 1}: {segmentEx.Message}", 
                                segmentEx);
                        }
                    }
                }

                // Verify we got pages
                if (segments.Count == 0)
                {
                    _logger.LogError("No document segments were extracted from {SourceFile} ({ByteCount} bytes). All extraction attempts failed.",
                        sourceFileName, pdfBytes.Length);
                    throw new InvalidOperationException(
                        $"Failed to extract any segments from {sourceFileName}. Check logs for details.");
                }

                return segments;
            }
            catch (InvalidOperationException ioEx)
            {
                _logger.LogWarning(ioEx, "Document splitting failed for {SourceFile}: {Message}. Attempting graceful fallback.",
                    sourceFileName, ioEx.Message);
                return AttemptGracefulFallback(pdfBytes, sourceFileName);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Document splitting failed for {SourceFile} ({ByteCount} bytes). {ExceptionType}: {Message}. Attempting graceful fallback.",
                    sourceFileName, pdfBytes.Length, ex.GetType().Name, ex.Message);
                return AttemptGracefulFallback(pdfBytes, sourceFileName);
            }
        }

        /// <summary>
        /// Calculates the document ranges based on detected boundaries.
        /// 
        /// Example: boundaries at [5, 8] with 18 total pages
        /// Returns: [(0-4, docType, conf), (5-7, docType, conf), (8-17, docType, conf)]
        /// </summary>
        private List<(int startPage, int endPage, string docType, double confidence)> CalculateDocumentRanges(
            List<DocumentBoundary> boundaries, int totalPages)
        {
            var ranges = new List<(int, int, string, double)>();
            
            if (boundaries.Count == 0)
            {
                _logger.LogDebug("No boundaries provided to CalculateDocumentRanges");
                return new List<(int, int, string, double)>
                {
                    (0, totalPages - 1, LoanDocumentTypes.Unknown, 0.0)
                };
            }

            // Sort boundaries by page number
            var sortedBoundaries = boundaries.OrderBy(b => b.PageNumber).ToList();
            _logger.LogDebug("Calculating document ranges from {BoundaryCount} boundaries", sortedBoundaries.Count);
            
            // Determine if we should skip the first boundary (i.e., treat page 0 as part of first document)
            // If first boundary is at page 0 or 1, we don't separate the cover page - merge it with next segment
            bool skipFirstBoundary = (sortedBoundaries[0].PageNumber <= 1);
            int startIndex = skipFirstBoundary ? 1 : 0;
            int currentStart = 0;
            
            _logger.LogDebug("First boundary at page {PageNum}, skipFirstBoundary={Skip}, startIndex={Index}", 
                sortedBoundaries[0].PageNumber, skipFirstBoundary, startIndex);
            
            // Process boundaries starting from startIndex
            for (int i = startIndex; i < sortedBoundaries.Count; i++)
            {
                var boundary = sortedBoundaries[i];
                
                // Add range from current start to this boundary
                string docType = sortedBoundaries[i > 0 ? i - 1 : 0].DocumentType;
                double confidence = sortedBoundaries[i > 0 ? i - 1 : 0].DocumentTypeConfidence;
                
                _logger.LogDebug("Range: pages {StartPage}-{EndPage} ({PageCount} pages) - Type: {DocType} (confidence: {Confidence:P1})",
                    currentStart + 1, boundary.PageNumber, boundary.PageNumber - currentStart, docType, confidence);
                
                ranges.Add((currentStart, boundary.PageNumber - 1, docType, confidence));
                currentStart = boundary.PageNumber;
            }

            // Add final range
            if (currentStart < totalPages)
            {
                var lastBoundary = sortedBoundaries.Last();
                _logger.LogDebug("Final range: pages {StartPage}-{EndPage} ({PageCount} pages) - Type: {DocType} (confidence: {Confidence:P1})",
                    currentStart + 1, totalPages, totalPages - currentStart, lastBoundary.DocumentType, lastBoundary.DocumentTypeConfidence);
                
                ranges.Add((currentStart, totalPages - 1, lastBoundary.DocumentType, lastBoundary.DocumentTypeConfidence));
            }

            _logger.LogInformation("Calculated {RangeCount} document ranges from {BoundaryCount} boundaries", ranges.Count, sortedBoundaries.Count);
            return ranges;
        }

        /// <summary>
        /// Extracts a specific range of pages from a PDF using iText7.
        /// Automatically filters out blank pages from the range using Document Intelligence.
        /// Throws InvalidOperationException if extraction fails (with full context).
        /// </summary>
        /// <param name="docIntelligenceResult">Optional Document Intelligence analysis for blank detection</param>
        private byte[] ExtractPageRange(byte[] pdfBytes, int startPage, int endPage, DocumentIntelligenceResult? docIntelligenceResult = null)
        {
            try
            {
                if (pdfBytes == null || pdfBytes.Length == 0)
                {
                    throw new ArgumentException("PDF bytes cannot be null or empty", nameof(pdfBytes));
                }

                if (startPage < 0 || endPage < startPage)
                {
                    throw new ArgumentException($"Invalid page range: startPage={startPage}, endPage={endPage}", nameof(startPage));
                }

                using (var sourceStream = new MemoryStream(pdfBytes))
                using (var pdfReader = new PdfReader(sourceStream))
                using (var sourcePdfDocument = new PdfDocument(pdfReader))
                {
                    int totalPages = sourcePdfDocument.GetNumberOfPages();

                    if (endPage >= totalPages)
                    {
                        _logger.LogWarning("Requested page range {StartPage}-{EndPage} exceeds total pages {TotalPages}. Adjusting endPage.",
                            startPage + 1, endPage + 1, totalPages);
                        endPage = totalPages - 1;
                    }

                    // Filter out blank pages from the range (if enabled and Document Intelligence available)
                    List<int> pagesToExtract;
                    int blanksRemoved = 0;
                    
                    if (EnableBlankPageRemoval && docIntelligenceResult != null)
                    {
                        var nonBlankPages = FilterBlankPages(docIntelligenceResult, startPage + 1, endPage + 1);
                        pagesToExtract = nonBlankPages;
                        blanksRemoved = (endPage - startPage + 1) - nonBlankPages.Count;
                        
                        if (nonBlankPages.Count == 0)
                        {
                            _logger.LogWarning("All pages in range {StartPage}-{EndPage} are blank (0 paragraphs/tables), skipping segment",
                                startPage + 1, endPage + 1);
                            return Array.Empty<byte>();
                        }
                        
                        if (blanksRemoved > 0)
                        {
                            _logger.LogInformation("Removed {BlankCount} blank page(s) from range {StartPage}-{EndPage} (Document Intelligence-based)",
                                blanksRemoved, startPage + 1, endPage + 1);
                        }
                    }
                    else
                    {
                        // Keep all pages (blank page removal disabled or no Document Intelligence data)
                        pagesToExtract = Enumerable.Range(startPage + 1, (endPage - startPage) + 1).ToList();
                    }

                    // Create output stream with compression
                    var outputStream = new MemoryStream();
                    var pdfWriter = new PdfWriter(outputStream, new WriterProperties().SetCompressionLevel(9));

                    // Copy selected pages to new PDF
                    using (var outputDocument = new PdfDocument(pdfWriter))
                    {
                        sourcePdfDocument.CopyPagesTo(pagesToExtract, outputDocument);
                    }

                    byte[] result = outputStream.ToArray();
                    _logger.LogDebug("Successfully extracted {PageCount} page(s). Output size: {ByteCount} bytes",
                        pagesToExtract.Count, result.Length);
                    
                    return result;
                }
            }
            catch (ArgumentException argEx)
            {
                _logger.LogError(argEx, "Invalid arguments to ExtractPageRange. StartPage={StartPage}, EndPage={EndPage}",
                    startPage, endPage);
                throw;
            }
            catch (IOException ioEx)
            {
                _logger.LogError(ioEx, "IO error while extracting pages {StartPage}-{EndPage}: {Message}",
                    startPage + 1, endPage + 1, ioEx.Message);
                throw new InvalidOperationException(
                    $"IO error extracting pages {startPage + 1}-{endPage + 1}: {ioEx.Message}", ioEx);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error extracting pages {StartPage}-{EndPage}: {ExceptionType}: {Message}",
                    startPage + 1, endPage + 1, ex.GetType().Name, ex.Message);
                throw new InvalidOperationException(
                    $"Failed to extract pages {startPage + 1}-{endPage + 1}: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Sanitizes a filename to remove invalid characters for Azure Blob Storage.
        /// </summary>
        private string SanitizeFileName(string fileName)
        {
            // Replace invalid characters with underscores
            var invalidChars = Path.GetInvalidFileNameChars().Concat(new[] { '/', '\\', '?', '*', '|', '"', '<', '>' });
            var result = fileName;
            foreach (var c in invalidChars)
            {
                result = result.Replace(c, '_');
            }
            return result;
        }

        /// <summary>
        /// Checks if a PDF page is blank using Document Intelligence paragraph and table counts.
        /// Works correctly with both text-based and image-based (scanned) PDFs.
        /// </summary>
        /// <param name="pageAnalysis">Document Intelligence page analysis (null means page not analyzed, treat as non-blank)</param>
        /// <param name="pageNumber">1-indexed page number for logging</param>
        /// <returns>True if page has 0 paragraphs AND 0 tables (truly blank)</returns>
        private bool IsBlankPage(PageAnalysis? pageAnalysis, int pageNumber)
        {
            if (pageAnalysis == null)
            {
                // No Document Intelligence data, keep the page
                return false;
            }
            
            int paragraphCount = pageAnalysis.Paragraphs?.Count ?? 0;
            int tableCount = pageAnalysis.Tables?.Count ?? 0;
            
            // Page is blank if it has no content structures
            bool isBlank = paragraphCount == 0 && tableCount == 0;
            
            if (isBlank)
            {
                _logger.LogInformation("Page {PageNumber} detected as blank (0 paragraphs, 0 tables)", pageNumber);
            }
            
            return isBlank;
        }

        /// <summary>
        /// Filters out blank pages from a page range using Document Intelligence analysis.
        /// Returns list of non-blank page numbers (1-indexed).
        /// </summary>
        /// <param name="docIntelligenceResult">Document Intelligence analysis result (null disables filtering)</param>
        /// <param name="startPage">1-indexed start page</param>
        /// <param name="endPage">1-indexed end page</param>
        private List<int> FilterBlankPages(DocumentIntelligenceResult? docIntelligenceResult, int startPage, int endPage)
        {
            var nonBlankPages = new List<int>();
            
            for (int pageNum = startPage; pageNum <= endPage; pageNum++)
            {
                // Find PageAnalysis for this page (Document Intelligence uses 0-indexed internally)
                PageAnalysis? pageAnalysis = docIntelligenceResult?.Pages?.FirstOrDefault(p => p.PageNumber == pageNum - 1);
                
                if (!IsBlankPage(pageAnalysis, pageNum))
                {
                    nonBlankPages.Add(pageNum);
                }
            }
            
            return nonBlankPages;
        }

        /// <summary>
        /// Attempts graceful fallback: returns entire PDF as single segment when splitting fails.
        /// </summary>
        private List<DocumentSegment> AttemptGracefulFallback(byte[] pdfBytes, string sourceFileName)
        {
            var segments = new List<DocumentSegment>();
            var fileNameBase = SanitizeFileName(Path.GetFileNameWithoutExtension(sourceFileName));
            
            try
            {
                using (var sourceStream = new MemoryStream(pdfBytes))
                using (var pdfReader = new PdfReader(sourceStream))
                using (var sourcePdfDocument = new PdfDocument(pdfReader))
                {
                    int totalPages = sourcePdfDocument.GetNumberOfPages();
                    var segment = new DocumentSegment
                    {
                        SegmentId = Guid.NewGuid().ToString("N").Substring(0, 12),
                        SourceFileName = sourceFileName,
                        StartPageNumber = 1,
                        EndPageNumber = totalPages,
                        PageCount = totalPages,
                        OutputBlobPath = $"split-documents/{fileNameBase}/segment-001.pdf",
                        SizeBytes = pdfBytes.Length,
                        CreatedAt = DateTime.UtcNow,
                        SegmentBytes = pdfBytes,
                        DocumentType = LoanDocumentTypes.Unknown,
                        DocumentTypeConfidence = 0.0,
                        BoundaryConfidence = 0.0
                    };
                    segments.Add(segment);
                    _logger.LogInformation("Graceful fallback successful: {SourceFile} ({PageCount} pages, {ByteCount} bytes) returned as single segment",
                        sourceFileName, totalPages, pdfBytes.Length);
                    return segments;
                }
            }
            catch (Exception fallbackEx)
            {
                _logger.LogCritical(fallbackEx, "CRITICAL: Even graceful fallback failed for {SourceFile}. Cannot recover. Error: {ExceptionType}: {Message}",
                    sourceFileName, fallbackEx.GetType().Name, fallbackEx.Message);
                throw new InvalidOperationException(
                    $"Complete failure: Document splitting failed AND graceful fallback failed for {sourceFileName}. See logs for details.",
                    fallbackEx);
            }
        }

        /// <summary>
        /// Splits PDF using boundaries provided from Document Intelligence analysis.
        /// This method leverages the enhanced boundary detection for more accurate splits.
        /// </summary>
        /// <param name="docIntelligenceResult">Optional Document Intelligence result for blank page detection</param>
        public List<DocumentSegment> SplitPdfWithBoundaries(byte[] pdfBytes, string sourceFileName, List<DocumentBoundary> boundaries, DocumentIntelligenceResult? docIntelligenceResult = null)
        {
            var segments = new List<DocumentSegment>();
            
            try
            {
                var fileNameBase = SanitizeFileName(Path.GetFileNameWithoutExtension(sourceFileName));

                using (var sourceStream = new MemoryStream(pdfBytes))
                using (var pdfReader = new PdfReader(sourceStream))
                using (var sourcePdfDocument = new PdfDocument(pdfReader))
                {
                    int totalPages = sourcePdfDocument.GetNumberOfPages();
                    
                    _logger.LogInformation("Splitting {FileName} ({PageCount} pages) using {BoundaryCount} AI-detected boundaries",
                        sourceFileName, totalPages, boundaries.Count);

                    // Calculate page ranges from boundaries
                    var ranges = CalculateDocumentRanges(boundaries, totalPages);
                    
                    int segmentNumber = 1;
                    int segmentIndex = 0;  // Track segment index for duplicate reference
                    
                    // Build a map of start page -> segment index for duplicate lookup
                    var startPageToSegmentIndex = new Dictionary<int, int>();
                    
                    foreach (var (startPage, endPage, docType, confidence) in ranges)
                    {
                        try
                        {
                            byte[] segmentBytes = ExtractPageRange(pdfBytes, startPage, endPage, docIntelligenceResult);

                            if (segmentBytes.Length > 0)
                            {
                                // Find the boundary for this segment's start page
                                var matchingBoundary = boundaries.FirstOrDefault(b => b.PageNumber == startPage);
                                bool isDuplicate = matchingBoundary?.IsDuplicateBoundary ?? false;
                                int? duplicateOfIndex = null;
                                
                                _logger.LogInformation("Segment {SegNum} check: startPage={Start}, boundaryFound={Found}, isDup={IsDup}",
                                    segmentNumber, startPage, matchingBoundary != null, isDuplicate);
                                
                                // If this is a duplicate, find the segment index it duplicates
                                if (isDuplicate && matchingBoundary?.DuplicateOfPageNumber.HasValue == true)
                                {
                                    int originalPage = matchingBoundary.DuplicateOfPageNumber.Value;
                                    if (startPageToSegmentIndex.TryGetValue(originalPage, out int origIndex))
                                    {
                                        duplicateOfIndex = origIndex;
                                        _logger.LogInformation("Segment {SegNum} (pages {StartPage}-{EndPage}) marked as duplicate of segment {OrigIndex} (page {OrigPage})",
                                            segmentNumber, startPage + 1, endPage + 1, origIndex + 1, originalPage + 1);
                                    }
                                }
                                
                                var segment = new DocumentSegment
                                {
                                    SegmentId = Guid.NewGuid().ToString("N").Substring(0, 12),
                                    SourceFileName = sourceFileName,
                                    StartPageNumber = startPage + 1,
                                    EndPageNumber = endPage + 1,
                                    PageCount = (endPage - startPage) + 1,
                                    OutputBlobPath = $"split-documents/{fileNameBase}/segment-{segmentNumber:D3}.pdf",
                                    SizeBytes = segmentBytes.Length,
                                    CreatedAt = DateTime.UtcNow,
                                    SegmentBytes = segmentBytes,
                                    DocumentType = docType,
                                    DocumentTypeConfidence = confidence,
                                    BoundaryConfidence = matchingBoundary?.ConfidenceScore ?? 0.0,
                                    DetectedAsDuplicate = isDuplicate,
                                    DuplicateOfSegmentIndex = duplicateOfIndex
                                };

                                // Track this segment's start page for future duplicate references
                                startPageToSegmentIndex[startPage] = segmentIndex;
                                
                                segments.Add(segment);
                                
                                _logger.LogInformation("Created segment {SegNum}: pages {StartPage}-{EndPage}, type: {DocType} (confidence: {Confidence:P1})",
                                    segmentNumber, startPage + 1, endPage + 1, docType, confidence);
                                
                                segmentNumber++;
                                segmentIndex++;
                            }
                        }
                        catch (Exception segmentEx)
                        {
                            _logger.LogError(segmentEx, "Failed to extract segment pages {StartPage}-{EndPage}",
                                startPage + 1, endPage + 1);
                        }
                    }
                }

                return segments;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Enhanced splitting failed, attempting fallback");
                // Fall back to original splitting logic
                return SplitPdf(pdfBytes, sourceFileName);
            }
        }
    }
}
------------------------
using DocumentProcessing.Models;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Enhanced boundary detector that uses Azure Document Intelligence results
    /// to identify logical document boundaries based on structure, headings, and content.
    /// </summary>
    public class EnhancedBoundaryDetectorService
    {
        private readonly ILogger<EnhancedBoundaryDetectorService>? _logger;

        // Contract/document keywords that indicate major boundaries
        private static readonly string[] MajorBoundaryKeywords = new[]
        {
            "agreement", "contract", "closing disclosure", "loan estimate",
            "deed of trust", "promissory note", "mortgage", "settlement statement",
            "purchase agreement", "lease agreement", "master service agreement",
            "annex", "exhibit", "schedule", "appendix", "form 1003"
        };

        // Section keywords that indicate minor boundaries
        private static readonly string[] SectionKeywords = new[]
        {
            "article", "section", "clause", "paragraph", "part",
            "terms and conditions", "definitions", "representations",
            "warranties", "covenants", "conditions precedent"
        };

        public EnhancedBoundaryDetectorService(ILogger<EnhancedBoundaryDetectorService>? logger = null)
        {
            _logger = logger;
        }

        /// <summary>
        /// Detects logical document boundaries from Document Intelligence analysis.
        /// Returns boundaries sorted by page number with confidence scores.
        /// Includes duplicate page detection via sequence restart and text pattern matching.
        /// </summary>
        public List<DocumentBoundary> DetectBoundaries(DocumentIntelligenceResult aiResult, byte[]? pdfBytes = null)
        {
            var boundaries = new List<DocumentBoundary>();

            _logger?.LogInformation("Detecting boundaries from Document Intelligence analysis: {PageCount} pages", 
                aiResult.PageCount);

            // Check if page 0 and page 1 are part of the same sequence
            // If they are (e.g., both have "Page X of 13"), don't create boundary at page 0
            bool page0IsSeparateDocument = true;
            if (aiResult.Pages.Count > 1)
            {
                // Try Footer/PageNumberText first, then fallback to paragraph content
                var page0Sequence = ExtractPageNumbering(aiResult.Pages[0].PageFooter) 
                                   ?? ExtractPageNumbering(aiResult.Pages[0].PageNumberText)
                                   ?? ExtractPageNumberFromParagraphs(aiResult.Pages[0]);
                
                var page1Sequence = ExtractPageNumbering(aiResult.Pages[1].PageFooter) 
                                   ?? ExtractPageNumbering(aiResult.Pages[1].PageNumberText)
                                   ?? ExtractPageNumberFromParagraphs(aiResult.Pages[1]);

                _logger?.LogDebug("Page 0 check: Footer='{Footer}', PageNum='{PageNum}', Extracted={Extracted}",
                    aiResult.Pages[0].PageFooter ?? "(null)",
                    aiResult.Pages[0].PageNumberText ?? "(null)",
                    page0Sequence.HasValue ? $"{page0Sequence.Value.current} of {page0Sequence.Value.total}" : "none");

                _logger?.LogDebug("Page 1 check: Footer='{Footer}', PageNum='{PageNum}', Extracted={Extracted}",
                    aiResult.Pages[1].PageFooter ?? "(null)",
                    aiResult.Pages[1].PageNumberText ?? "(null)",
                    page1Sequence.HasValue ? $"{page1Sequence.Value.current} of {page1Sequence.Value.total}" : "none");

                if (page0Sequence.HasValue && page1Sequence.HasValue)
                {
                    if (page0Sequence.Value.total == page1Sequence.Value.total)
                    {
                        page0IsSeparateDocument = false;
                        _logger?.LogInformation("✅ Page 0 is part of same sequence as page 1 (both part of '{Total}' page document) - not creating boundary at page 0",
                            page0Sequence.Value.total);
                    }
                    else
                    {
                        _logger?.LogInformation("Page 0 and page 1 have DIFFERENT sequences ({Seq0} vs {Seq1}) - creating boundary at page 0",
                            page0Sequence.Value.total, page1Sequence.Value.total);
                    }
                }
                else
                {
                    _logger?.LogDebug("Page 0 or page 1 missing sequence numbers - creating boundary at page 0 (page0={Has0}, page1={Has1})",
                        page0Sequence.HasValue, page1Sequence.HasValue);
                }
            }

            // Only add page 0 as boundary if it's a separate document
            if (page0IsSeparateDocument)
            {
                boundaries.Add(new DocumentBoundary
                {
                    PageNumber = 0,
                    ConfidenceScore = 1.0,
                    DocumentType = "Unknown",
                    DocumentTypeConfidence = 0.0,
                    BoundarySignals = new List<BoundarySignal>
                    {
                        new BoundarySignal
                        {
                            SignalType = BoundarySignalType.StrongTitle,
                            Description = "First page of document",
                            Weight = 1.0,
                            SupportsNewBoundary = true
                        }
                    }
                });
            }

            (int current, int total)? previousPageSequence = null;

            // Track page text patterns for duplicate detection (rolling window)
            var pageTextPatterns = new Dictionary<int, string>();  // pageNum -> first 50 words
            int patternWindowSize = 20;  // Look back up to 20 pages for duplicate patterns

            // Track duplicate section to avoid creating boundaries within duplicate
            int skipBoundaryUntilPage = -1;  // If > current page, skip boundary detection (we're inside a duplicate section)

            // Track expected document end - when we see "Page 1 of X", expect X pages
            // Don't create doc-type boundaries until we pass expected end
            int expectedDocumentEndPage = -1;
            int expectedDocumentTotal = 0;

            // If page 0 is part of a sequence, initialize the tracker AND set expected document end
            if (!page0IsSeparateDocument && aiResult.Pages.Count > 0)
            {
                previousPageSequence = ExtractPageNumbering(aiResult.Pages[0].PageFooter) 
                                      ?? ExtractPageNumbering(aiResult.Pages[0].PageNumberText);
                
                // Set expected document end for page 0's document
                if (previousPageSequence.HasValue && previousPageSequence.Value.current == 1)
                {
                    expectedDocumentEndPage = previousPageSequence.Value.total - 1;  // 0-indexed
                    expectedDocumentTotal = previousPageSequence.Value.total;
                    _logger?.LogDebug("Page 0 starts a {Total}-page document. Expecting it to end at page {EndPage}",
                        expectedDocumentTotal, expectedDocumentEndPage);
                }
            }

            // Analyze each page for boundaries
            for (int pageNum = 1; pageNum < aiResult.Pages.Count; pageNum++)
            {
                var currentPage = aiResult.Pages[pageNum];
                var previousPage = aiResult.Pages[pageNum - 1];

                // Extract current page sequence for boundary detection
                // Try multiple sources: Footer, PageNumberText, then search paragraphs as fallback
                var currentPageSequence = ExtractPageNumbering(currentPage.PageFooter) 
                                         ?? ExtractPageNumbering(currentPage.PageNumberText)
                                         ?? ExtractPageNumberFromParagraphs(currentPage);

                // Skip boundary detection if we're inside a detected duplicate section
                if (pageNum < skipBoundaryUntilPage)
                {
                    _logger?.LogDebug("Page {PageNum}: Skipping boundary detection - inside duplicate section (until page {SkipUntil})",
                        pageNum, skipBoundaryUntilPage);
                    
                    // Still update previous sequence tracker
                    if (currentPageSequence.HasValue)
                    {
                        previousPageSequence = currentPageSequence;
                    }
                    continue;
                }

                bool shouldCreateBoundary = false;
                string reason = "";
                DocumentBoundary? boundary = null;
                bool isDuplicateBoundary = false;
                int? duplicateOfPage = null;

                // Log detailed info for every page for debugging
                _logger?.LogDebug("Page {PageNum}: currentSeq={CurrentSeq}, prevSeq={PrevSeq}, expectedEnd={ExpEnd}",
                    pageNum,
                    currentPageSequence.HasValue ? $"{currentPageSequence.Value.current}/{currentPageSequence.Value.total}" : "none",
                    previousPageSequence.HasValue ? $"{previousPageSequence.Value.current}/{previousPageSequence.Value.total}" : "none",
                    expectedDocumentEndPage);

                // PRIORITY 1: Check if page sequence RESTARTED with same total (duplicate document)
                // This check runs INDEPENDENTLY of AnalyzePageForBoundary
                // Example: "Page 10 of 10" → "Page 1 of 10" indicates duplicate 10-page document
                if (currentPageSequence.HasValue && previousPageSequence.HasValue)
                    {
                        // Check for sequence restart: current page is 1, same total as previous
                        // This detects restart even if previous page wasn't extracted as the last page
                        if (currentPageSequence.Value.current == 1 &&
                            currentPageSequence.Value.total == previousPageSequence.Value.total &&
                            previousPageSequence.Value.current > 1)  // Previous was not page 1 (avoid false positive on first pages)
                        {
                            // Sequence restarted with same total - potential duplicate or new document!
                            // Hash verify before creating boundary
                            int potentialDuplicateStartPage = pageNum;
                            int originalStartPage = pageNum - previousPageSequence.Value.total;
                            int duplicateSectionLength = currentPageSequence.Value.total;
                            
                            _logger?.LogDebug("Potential duplicate detected at page {PageNum}: sequence restarted (1 of {Total}). Previous was {PrevCurrent} of {PrevTotal}. Verifying with text content hash...",
                                pageNum, currentPageSequence.Value.total, previousPageSequence.Value.current, previousPageSequence.Value.total);
                            
                            // Always create boundary on sequence restart - it's clearly a new document
                            shouldCreateBoundary = true;
                            
                            // Set expected document end for this new document
                            expectedDocumentEndPage = pageNum + currentPageSequence.Value.total - 1;
                            expectedDocumentTotal = currentPageSequence.Value.total;
                            _logger?.LogDebug("Expecting {Total}-page document ending at page {EndPage}",
                                expectedDocumentTotal, expectedDocumentEndPage);
                            
                            // Check if it's a duplicate for logging purposes
                            if (originalStartPage >= 0 && VerifyDuplicatePagesViaHash(aiResult, originalStartPage, potentialDuplicateStartPage, duplicateSectionLength))
                            {
                                reason = $"Duplicate document detected: sequence restarted (1 of {currentPageSequence.Value.total}) with matching content";
                                _logger?.LogInformation("✅ Hash verification confirmed duplicate at page {PageNum} (duplicate of page {OriginalPage})", 
                                    pageNum, originalStartPage);
                                
                                // Mark this as a duplicate boundary
                                isDuplicateBoundary = true;
                                duplicateOfPage = originalStartPage;
                                
                                // Create the boundary object here for PRIORITY 1 duplicates
                                boundary = new DocumentBoundary
                                {
                                    PageNumber = pageNum,
                                    ConfidenceScore = 1.0,
                                    DocumentType = "Unknown",
                                    DocumentTypeConfidence = 1.0,
                                    IsDuplicateBoundary = true,
                                    DuplicateOfPageNumber = originalStartPage,
                                    BoundarySignals = new List<BoundarySignal>
                                    {
                                        new BoundarySignal
                                        {
                                            SignalType = BoundarySignalType.StrongTitle,
                                            Description = reason,
                                            Weight = 1.0,
                                            SupportsNewBoundary = true
                                        }
                                    }
                                };
                                
                                // Skip boundary detection for remaining pages in this duplicate section
                                skipBoundaryUntilPage = pageNum + duplicateSectionLength;
                                _logger?.LogDebug("Will skip boundary detection for pages {Start}-{End} (duplicate section)",
                                    pageNum + 1, skipBoundaryUntilPage - 1);
                            }
                            else
                            {
                                reason = $"New document detected: sequence restarted (1 of {currentPageSequence.Value.total})";
                                _logger?.LogInformation("✅ Sequence restart detected at page {PageNum} - new document (not duplicate)", pageNum);
                                
                                // Create boundary for non-duplicate sequence restart
                                boundary = new DocumentBoundary
                                {
                                    PageNumber = pageNum,
                                    ConfidenceScore = 1.0,
                                    DocumentType = "Unknown",
                                    DocumentTypeConfidence = 1.0,
                                    BoundarySignals = new List<BoundarySignal>
                                    {
                                        new BoundarySignal
                                        {
                                            SignalType = BoundarySignalType.StrongTitle,
                                            Description = reason,
                                            Weight = 1.0,
                                            SupportsNewBoundary = true
                                        }
                                    }
                                };
                            }
                        }
                        // Check for page number going BACKWARDS (e.g., "Page 5 of 6" → "Page 1 of 6" or any lower number)
                        else if (currentPageSequence.Value.current < previousPageSequence.Value.current &&
                                 currentPageSequence.Value.total == previousPageSequence.Value.total)
                        {
                            // Page number went backwards with same total - this is a new document!
                            shouldCreateBoundary = true;
                            reason = $"New document detected: page number reset ({previousPageSequence.Value.current} → {currentPageSequence.Value.current} of {currentPageSequence.Value.total})";
                            _logger?.LogInformation("✅ Page number reset detected at page {PageNum}: {Prev} → {Curr} of {Total}",
                                pageNum, previousPageSequence.Value.current, currentPageSequence.Value.current, currentPageSequence.Value.total);
                            
                            // Set expected document end for this new document
                            if (currentPageSequence.Value.current == 1)
                            {
                                expectedDocumentEndPage = pageNum + currentPageSequence.Value.total - 1;
                                expectedDocumentTotal = currentPageSequence.Value.total;
                                _logger?.LogDebug("Expecting {Total}-page document ending at page {EndPage}",
                                    expectedDocumentTotal, expectedDocumentEndPage);
                            }
                        }
                        // Check for sequence total change (different document)
                        else if (currentPageSequence.Value.total != previousPageSequence.Value.total)
                        {
                            shouldCreateBoundary = true;
                            reason = $"Page sequence changed: {previousPageSequence.Value.total} → {currentPageSequence.Value.total}";
                            
                            // Set expected document end for this new document
                            if (currentPageSequence.Value.current == 1)
                            {
                                expectedDocumentEndPage = pageNum + currentPageSequence.Value.total - 1;
                                expectedDocumentTotal = currentPageSequence.Value.total;
                                _logger?.LogDebug("Expecting {Total}-page document ending at page {EndPage}",
                                    expectedDocumentTotal, expectedDocumentEndPage);
                            }
                        }
                    }
                    // Also check: current page is Page 1 of X but no previous sequence (first numbered page)
                    else if (currentPageSequence.HasValue && currentPageSequence.Value.current == 1 && !previousPageSequence.HasValue)
                    {
                        // This is Page 1 of a new document - set expected end
                        expectedDocumentEndPage = pageNum + currentPageSequence.Value.total - 1;
                        expectedDocumentTotal = currentPageSequence.Value.total;
                        _logger?.LogDebug("First numbered page detected: Page 1 of {Total}. Expecting document to end at page {EndPage}",
                            currentPageSequence.Value.total, expectedDocumentEndPage);
                    }

                    // PRIORITY 2: Check for text pattern match (duplicate without page numbers)
                    // Also check when page numbers exist but we're past the expected document end
                    bool pastExpectedEnd = (expectedDocumentEndPage >= 0 && pageNum > expectedDocumentEndPage);
                    bool noPageNumbers = (!currentPageSequence.HasValue && !previousPageSequence.HasValue);
                    
                    if (!shouldCreateBoundary && (noPageNumbers || pastExpectedEnd))
                    {
                        // Use text pattern matching for duplicate detection
                        string currentPattern = ExtractPageTextPattern(currentPage);
                        pageTextPatterns[pageNum] = currentPattern;
                        
                        // Look back through recent pages for matching pattern
                        for (int lookbackPages = 5; lookbackPages <= Math.Min(patternWindowSize, pageNum - 1); lookbackPages++)
                        {
                            int comparePageNum = pageNum - lookbackPages;
                            if (pageTextPatterns.ContainsKey(comparePageNum))
                            {
                                string comparePattern = pageTextPatterns[comparePageNum];
                                
                                // Check if patterns are similar (>80% match)
                                if (ArePatternsSimular(currentPattern, comparePattern, 0.8))
                                {
                                    _logger?.LogDebug("Potential duplicate detected at page {PageNum}: text pattern matches page {ComparePageNum}. Verifying with hash...",
                                        pageNum, comparePageNum);
                                    
                                    // Hash verify the sections
                                    int duplicateSectionLength = lookbackPages;
                                    if (VerifyDuplicatePagesViaHash(aiResult, comparePageNum, pageNum, duplicateSectionLength))
                                    {
                                        shouldCreateBoundary = true;
                                        reason = $"Duplicate section detected: text pattern matches starting at page {comparePageNum + 1}";
                                        _logger?.LogInformation("✅ Hash verification confirmed duplicate at page {PageNum} (matches page {ComparePageNum})", 
                                            pageNum, comparePageNum);
                                        
                                        // Skip boundary detection for remaining pages in this duplicate section
                                        skipBoundaryUntilPage = pageNum + duplicateSectionLength;
                                        _logger?.LogDebug("Will skip boundary detection for pages {Start}-{End} (duplicate section)",
                                            pageNum + 1, skipBoundaryUntilPage - 1);
                                        break;  // Found a match, no need to check further
                                    }
                                }
                            }
                        }
                    }

                    // PRIORITY 3: If no duplicate detected, check document type change via AnalyzePageForBoundary
                    if (!shouldCreateBoundary)
                    {
                        boundary = AnalyzePageForBoundary(currentPage, previousPage, pageNum);
                        
                        if (boundary != null && boundary.ConfidenceScore >= 0.5)
                        {
                            string? previousDocType = boundaries.Count > 0 ? boundaries.Last().DocumentType : null;
                            int lastBoundaryPage = boundaries.Count > 0 ? boundaries.Last().PageNumber : -1;
                            
                            // Avoid creating consecutive boundaries (would result in 1-page orphan segments)
                            // Exception: Allow if current page has "Page 1 of X" pattern (legitimate new document)
                            bool hasPage1Pattern = currentPageSequence.HasValue && currentPageSequence.Value.current == 1;
                            bool isConsecutiveBoundary = (lastBoundaryPage == pageNum - 1);
                            
                            // Also avoid creating boundaries if we're inside an expected document range
                            bool insideExpectedDocument = (expectedDocumentEndPage >= 0 && pageNum <= expectedDocumentEndPage);
                            
                            // CRITICAL: "Page 1 of X" pattern ALWAYS creates a boundary (new document start)
                            // This takes precedence over doc type matching
                            if (hasPage1Pattern)
                            {
                                // Check if previous page also had Page 1 - this is two separate 1-page docs
                                bool prevHasPage1Pattern = previousPageSequence.HasValue && previousPageSequence.Value.current == 1;
                                
                                if (isConsecutiveBoundary && !prevHasPage1Pattern)
                                {
                                    // Would create orphan, but we have Page 1 - still skip
                                    _logger?.LogDebug("Page {PageNum}: Skipping Page 1 boundary - would create 1-page orphan after page {LastBoundary}",
                                        pageNum, lastBoundaryPage);
                                }
                                else
                                {
                                    shouldCreateBoundary = true;
                                    reason = prevHasPage1Pattern 
                                        ? $"New document start: Page 1 of {currentPageSequence.Value.total} (previous was also Page 1 - separate documents)"
                                        : $"New document start: Page 1 of {currentPageSequence.Value.total}";
                                    
                                    // DUPLICATE CHECK: For single-page documents (1 of 1), check if content matches previous single-page doc
                                    if (prevHasPage1Pattern && currentPageSequence.Value.total == 1 && previousPageSequence.Value.total == 1)
                                    {
                                        // Both current and previous are single-page documents - check for duplicate content
                                        var currPageData = aiResult?.Pages?.ElementAtOrDefault(pageNum);
                                        var prevPageData = aiResult?.Pages?.ElementAtOrDefault(pageNum - 1);
                                        
                                        if (currPageData != null && prevPageData != null)
                                        {
                                            var currentPageText = string.Join(" ", currPageData.Paragraphs.Select(p => p.Content));
                                            var previousPageText = string.Join(" ", prevPageData.Paragraphs.Select(p => p.Content));
                                            
                                            if (!string.IsNullOrWhiteSpace(currentPageText) && !string.IsNullOrWhiteSpace(previousPageText))
                                            {
                                                var similarity = CalculateTextSimilarity(currentPageText, previousPageText);
                                                
                                                _logger?.LogDebug("Single-page duplicate check at page {PageNum}: similarity {Similarity:P1}",
                                                    pageNum, similarity);
                                                
                                                if (similarity >= 0.85)
                                                {
                                                    isDuplicateBoundary = true;
                                                    duplicateOfPage = pageNum - 1;
                                                    reason = $"Single-page duplicate detected: Page 1 of 1 [DUPLICATE of page {pageNum - 1}]";
                                                    _logger?.LogInformation("✅ Single-page duplicate detected at page {PageNum} (duplicate of page {PrevPage})",
                                                        pageNum, pageNum - 1);
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Update expected document end
                                    expectedDocumentEndPage = pageNum + currentPageSequence.Value.total - 1;
                                    expectedDocumentTotal = currentPageSequence.Value.total;
                                    _logger?.LogDebug("Page 1 detected: expecting {Total}-page document ending at page {EndPage}",
                                        expectedDocumentTotal, expectedDocumentEndPage);
                                }
                            }
                            else if (previousDocType != boundary.DocumentType)
                            {
                                if (isConsecutiveBoundary)
                                {
                                    // Skip this boundary to avoid creating 1-page orphan
                                    _logger?.LogDebug("Page {PageNum}: Skipping document type boundary ({Prev} → {Curr}) - would create 1-page orphan after page {LastBoundary}",
                                        pageNum, previousDocType, boundary.DocumentType, lastBoundaryPage);
                                }
                                else if (insideExpectedDocument)
                                {
                                    // Skip this boundary - we're still inside an expected document
                                    _logger?.LogDebug("Page {PageNum}: Skipping document type boundary ({Prev} → {Curr}) - inside expected {Total}-page document (ends at page {EndPage})",
                                        pageNum, previousDocType, boundary.DocumentType, expectedDocumentTotal, expectedDocumentEndPage);
                                }
                                else
                                {
                                    shouldCreateBoundary = true;
                                    reason = $"Document type changed: {previousDocType} → {boundary.DocumentType}";
                                }
                            }
                            else
                            {
                                _logger?.LogDebug("Page {PageNum}: Same document type '{DocType}' and no Page 1 pattern - not reporting as boundary",
                                    pageNum, boundary.DocumentType);
                            }
                        }
                    }
                    else
                    {
                        // Duplicate detected - create boundary manually
                        boundary = new DocumentBoundary
                        {
                            PageNumber = pageNum,
                            ConfidenceScore = 1.0,
                            DocumentType = "Unknown",
                            DocumentTypeConfidence = 1.0,
                            BoundarySignals = new List<BoundarySignal>
                            {
                                new BoundarySignal
                                {
                                    SignalType = BoundarySignalType.StrongTitle,
                                    Description = reason,
                                    Weight = 1.0,
                                    SupportsNewBoundary = true
                                }
                            }
                        };
                    }

                    if (shouldCreateBoundary && boundary != null)
                    {
                        // Set duplicate flags if this boundary marks a duplicate document
                        if (isDuplicateBoundary)
                        {
                            boundary.IsDuplicateBoundary = true;
                            boundary.DuplicateOfPageNumber = duplicateOfPage;
                        }
                        
                        boundaries.Add(boundary);
                        _logger?.LogInformation("✅ Detected boundary at page {PageNum}: {DocType} (confidence: {Confidence:P1}) - {Reason}{DupInfo}",
                            pageNum, boundary.DocumentType, boundary.ConfidenceScore, reason,
                            isDuplicateBoundary ? $" [DUPLICATE of page {duplicateOfPage}]" : "");
                    }

                // Update previous sequence tracker
                if (currentPageSequence.HasValue)
                {
                    previousPageSequence = currentPageSequence;
                }
            }

            _logger?.LogInformation("Enhanced boundary detection complete: {BoundaryCount} boundaries found", boundaries.Count);
            return boundaries.OrderBy(b => b.PageNumber).ToList();
        }

        /// <summary>
        /// Extracts first N words from a page as a text pattern for duplicate detection.
        /// Returns normalized text (lowercase, whitespace cleaned) for comparison.
        /// </summary>
        private string ExtractPageTextPattern(PageAnalysis page, int maxWords = 50)
        {
            var words = new List<string>();
            
            // Extract from paragraphs first (most common content)
            if (page.Paragraphs != null)
            {
                foreach (var paragraph in page.Paragraphs)
                {
                    if (!string.IsNullOrWhiteSpace(paragraph.Content))
                    {
                        var paragraphWords = paragraph.Content
                            .Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries)
                            .Select(w => w.ToLowerInvariant().Trim())
                            .Where(w => w.Length > 2);  // Filter out very short words
                        
                        words.AddRange(paragraphWords);
                        
                        if (words.Count >= maxWords)
                            break;
                    }
                }
            }
            
            // Take first maxWords words and join
            return string.Join(" ", words.Take(maxWords));
        }

        /// <summary>
        /// Checks if two text patterns are similar based on word overlap.
        /// Uses Jaccard similarity: intersection / union of word sets.
        /// </summary>
        private bool ArePatternsSimular(string pattern1, string pattern2, double threshold = 0.8)
        {
            if (string.IsNullOrWhiteSpace(pattern1) || string.IsNullOrWhiteSpace(pattern2))
                return false;
            
            var words1 = new HashSet<string>(pattern1.Split(' ', StringSplitOptions.RemoveEmptyEntries));
            var words2 = new HashSet<string>(pattern2.Split(' ', StringSplitOptions.RemoveEmptyEntries));
            
            if (words1.Count == 0 || words2.Count == 0)
                return false;
            
            var intersection = words1.Intersect(words2).Count();
            var union = words1.Union(words2).Count();
            
            double similarity = (double)intersection / union;
            return similarity >= threshold;
        }

        /// <summary>
        /// Verifies if two page sections are duplicates by comparing their text content similarity.
        /// Uses Document Intelligence paragraph content with similarity threshold to handle minor extraction differences.
        /// </summary>
        private bool VerifyDuplicatePagesViaHash(DocumentIntelligenceResult aiResult, int startPage1, int startPage2, int pageCount)
        {
            try
            {
                // Validate page indices before extraction
                if (startPage1 < 0 || startPage2 < 0 || pageCount <= 0)
                {
                    _logger?.LogDebug("Invalid page indices for similarity verification: startPage1={P1}, startPage2={P2}, pageCount={Count}",
                        startPage1, startPage2, pageCount);
                    return false;
                }
                
                if (startPage1 >= aiResult.Pages.Count || startPage2 >= aiResult.Pages.Count)
                {
                    _logger?.LogDebug("Page indices exceed available pages: startPage1={P1}, startPage2={P2}, totalPages={Total}",
                        startPage1, startPage2, aiResult.Pages.Count);
                    return false;
                }
                
                // Extract text content from both sections (compare 5 pages or full pageCount)
                int pagesToCompare = Math.Min(5, pageCount);
                
                var text1 = ExtractTextFromPageRange(aiResult, startPage1, Math.Min(startPage1 + pagesToCompare - 1, aiResult.Pages.Count - 1));
                var text2 = ExtractTextFromPageRange(aiResult, startPage2, Math.Min(startPage2 + pagesToCompare - 1, aiResult.Pages.Count - 1));
                
                if (string.IsNullOrWhiteSpace(text1) || string.IsNullOrWhiteSpace(text2))
                {
                    _logger?.LogDebug("Could not extract text for comparison");
                    return false;
                }
                
                // Normalize text (lowercase, remove extra whitespace, remove page numbers)
                text1 = NormalizeTextForDuplicateDetection(text1);
                text2 = NormalizeTextForDuplicateDetection(text2);
                
                // Calculate similarity using word-level comparison
                double similarity = CalculateTextSimilarity(text1, text2);
                
                // Consider duplicate if >85% similar
                bool isMatch = similarity >= 0.85;
                
                _logger?.LogDebug("Text similarity comparison: {Similarity:P1} - Match: {IsMatch} (threshold: 85%)",
                    similarity, isMatch);
                
                return isMatch;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Error during similarity verification at pages {Page1} and {Page2}", startPage1, startPage2);
                return false;  // On error, don't create boundary
            }
        }

        /// <summary>
        /// Extracts all text content from a page range in Document Intelligence results.
        /// </summary>
        private string ExtractTextFromPageRange(DocumentIntelligenceResult aiResult, int startPage, int endPage)
        {
            var textBuilder = new System.Text.StringBuilder();
            
            for (int i = startPage; i <= endPage && i < aiResult.Pages.Count; i++)
            {
                var page = aiResult.Pages[i];
                
                // Extract from paragraphs
                if (page.Paragraphs != null)
                {
                    foreach (var para in page.Paragraphs)
                    {
                        if (!string.IsNullOrWhiteSpace(para.Content))
                        {
                            textBuilder.AppendLine(para.Content);
                        }
                    }
                }
            }
            
            return textBuilder.ToString();
        }

        /// <summary>
        /// Normalizes text for duplicate detection: lowercase, remove page numbers, remove special chars, normalize whitespace.
        /// </summary>
        private string NormalizeTextForDuplicateDetection(string text)
        {
            if (string.IsNullOrWhiteSpace(text))
                return string.Empty;
            
            // Remove page number patterns (e.g., "Page 1 of 10", "1 of 10", "Page 1")
            text = System.Text.RegularExpressions.Regex.Replace(text, @"page\s+\d+\s+of\s+\d+", "", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            text = System.Text.RegularExpressions.Regex.Replace(text, @"\d+\s+of\s+\d+", "");
            
            // Lowercase
            text = text.ToLowerInvariant();
            
            // Remove special characters except letters, numbers, spaces
            text = System.Text.RegularExpressions.Regex.Replace(text, @"[^a-z0-9\s]", "");
            
            // Normalize whitespace
            text = System.Text.RegularExpressions.Regex.Replace(text, @"\s+", " ").Trim();
            
            return text;
        }

        /// <summary>
        /// Calculates text similarity using Jaccard similarity on word sets.
        /// Returns value between 0.0 (no match) and 1.0 (perfect match).
        /// </summary>
        private double CalculateTextSimilarity(string text1, string text2)
        {
            if (string.IsNullOrWhiteSpace(text1) || string.IsNullOrWhiteSpace(text2))
                return 0.0;
            
            var words1 = new HashSet<string>(text1.Split(' ', StringSplitOptions.RemoveEmptyEntries));
            var words2 = new HashSet<string>(text2.Split(' ', StringSplitOptions.RemoveEmptyEntries));
            
            if (words1.Count == 0 || words2.Count == 0)
                return 0.0;
            
            var intersection = words1.Intersect(words2).Count();
            var union = words1.Union(words2).Count();
            
            return (double)intersection / union;
        }

        /// <summary>
        /// Normalizes text for comparison: lowercase, remove extra whitespace, remove special chars.
        /// </summary>
        private string NormalizeText(string text)
        {
            if (string.IsNullOrWhiteSpace(text))
                return string.Empty;
            
            // Lowercase
            text = text.ToLowerInvariant();
            
            // Remove special characters except letters, numbers, spaces
            text = System.Text.RegularExpressions.Regex.Replace(text, @"[^a-z0-9\s]", "");
            
            // Normalize whitespace
            text = System.Text.RegularExpressions.Regex.Replace(text, @"\s+", " ").Trim();
            
            return text;
        }

        /// <summary>
        /// Computes SHA-256 hash for quick comparison.
        /// </summary>
        private string ComputeQuickHash(byte[] data)
        {
            using (var sha256 = System.Security.Cryptography.SHA256.Create())
            {
                var hashBytes = sha256.ComputeHash(data);
                return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
            }
        }

        /// <summary>
        /// Extracts page numbering pattern from text (e.g., "Page 3 of 13" returns (3, 13)).
        /// Returns null if no pattern found.
        /// Uses aggressive text normalization to handle Unicode issues, zero-width characters, etc.
        /// </summary>
        private (int current, int total)? ExtractPageNumbering(string? text)
        {
            if (string.IsNullOrWhiteSpace(text))
                return null;

            // AGGRESSIVE CLEANING: Strip ALL non-printable and special Unicode characters
            // Keep only: printable ASCII (0x20-0x7E), which includes letters, digits, basic punctuation
            var cleanText = System.Text.RegularExpressions.Regex.Replace(text, @"[^\x20-\x7E]", " ");
            
            // Normalize all whitespace sequences to single space
            cleanText = System.Text.RegularExpressions.Regex.Replace(cleanText, @"\s+", " ").Trim();

            // Skip recording stamps (INSTR. #xxx Page Number: X of Y) - these are not document page numbers
            if (System.Text.RegularExpressions.Regex.IsMatch(cleanText, @"INSTR\.?\s*#?\s*\d+", 
                System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            {
                _logger?.LogDebug("Skipping recording stamp text: '{CleanText}'", 
                    cleanText.Length > 80 ? cleanText.Substring(0, 80) + "..." : cleanText);
                return null;
            }

            // Try multiple patterns to handle variations
            var patterns = new[]
            {
                @"[Pp]age\s+(\d+)\s+of\s+(\d+)",    // "Page X of Y"
                @"[Pp]age\s+(\d+)\s*/\s*(\d+)",     // "Page X/Y"
                @"[Pp]age\s*[-:]\s*(\d+)\s+of\s+(\d+)",  // "Page: X of Y" or "Page - X of Y"
                @"^(\d+)\s+of\s+(\d+)$",             // "X of Y" - for footers without "Page" prefix (e.g., "1 of 11")
            };

            foreach (var pattern in patterns)
            {
                var match = System.Text.RegularExpressions.Regex.Match(cleanText, pattern,
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                if (match.Success &&
                    int.TryParse(match.Groups[1].Value, out int current) &&
                    int.TryParse(match.Groups[2].Value, out int total))
                {
                    _logger?.LogDebug("Extracted page pattern: {Current} of {Total} from text: '{OriginalText}' -> '{CleanText}'",
                        current, total, text.Length > 100 ? text.Substring(0, 100) + "..." : text, 
                        cleanText.Length > 100 ? cleanText.Substring(0, 100) + "..." : cleanText);
                    return (current, total);
                }
            }

            // Log extraction failure for debugging
            if (cleanText.Contains("of") && System.Text.RegularExpressions.Regex.IsMatch(cleanText, @"\d"))
            {
                _logger?.LogDebug("Failed to extract page pattern from: '{CleanText}' (original: '{OriginalText}')",
                    cleanText.Length > 100 ? cleanText.Substring(0, 100) : cleanText,
                    text.Length > 100 ? text.Substring(0, 100) : text);
            }

            return null;
        }

        /// <summary>
        /// Fallback: Extract page numbering from paragraph content when Footer/PageNumberText are empty.
        /// Searches through all paragraphs on the page for page number patterns.
        /// </summary>
        private (int current, int total)? ExtractPageNumberFromParagraphs(PageAnalysis page)
        {
            if (page.Paragraphs == null || page.Paragraphs.Count == 0)
                return null;

            // Search through all paragraphs for page number pattern
            foreach (var paragraph in page.Paragraphs)
            {
                if (string.IsNullOrWhiteSpace(paragraph.Content))
                    continue;

                var result = ExtractPageNumbering(paragraph.Content);
                if (result.HasValue)
                {
                    _logger?.LogDebug("Found page number in paragraph: '{Preview}' -> {Current} of {Total}",
                        paragraph.Content.Length > 50 ? paragraph.Content.Substring(0, 50) + "..." : paragraph.Content,
                        result.Value.current, result.Value.total);
                    return result;
                }
            }

            return null;
        }

        private DocumentBoundary? AnalyzePageForBoundary(PageAnalysis currentPage, PageAnalysis previousPage, int pageNumber)
        {
            // CRITICAL: Check if pages are part of same numbered sequence (Page X of Y) FIRST
            // If both have "X of Y" pattern with SAME total, they're the same document
            // This is the PRIMARY signal - if detected, no boundary regardless of other signals
            
            // Try extracting from BOTH footer and page number text (not just first non-null)
            var currentPageNum = ExtractPageNumbering(currentPage.PageFooter) 
                                ?? ExtractPageNumbering(currentPage.PageNumberText);
            var previousPageNum = ExtractPageNumbering(previousPage.PageFooter) 
                                 ?? ExtractPageNumbering(previousPage.PageNumberText);

            // Debug logging to see what we extracted
            _logger?.LogDebug("Page {PageNum}: Footer='{Footer}', PageNum='{PageNumText}', Extracted={Extracted}",
                pageNumber, 
                currentPage.PageFooter ?? "(null)", 
                currentPage.PageNumberText ?? "(null)",
                currentPageNum.HasValue ? $"{currentPageNum.Value.current} of {currentPageNum.Value.total}" : "none");

            if (currentPageNum.HasValue && previousPageNum.HasValue)
            {
                // CRITICAL FIX: If both pages are "Page 1 of X", they are DIFFERENT documents!
                // Example: "Page 1 of 1" followed by "Page 1 of 1" = two separate 1-page documents
                if (currentPageNum.Value.current == 1 && previousPageNum.Value.current == 1)
                {
                    // Both are page 1 - these are separate documents, NOT the same sequence
                    _logger?.LogDebug("Page {PageNum}: Both current and previous are Page 1 - treating as separate documents",
                        pageNumber);
                    // Don't return null - continue to create boundary
                }
                else if (currentPageNum.Value.total == previousPageNum.Value.total &&
                         currentPageNum.Value.current > previousPageNum.Value.current)
                {
                    // Pages belong to same numbered sequence (e.g., Page 2 of 10 after Page 1 of 10) - NO BOUNDARY
                    _logger?.LogDebug("Page {PageNum}: Part of same sequence (page {Current} of {Total}) - NO boundary",
                        pageNumber, currentPageNum.Value.current, currentPageNum.Value.total);
                    return null;  // Early exit - page sequence overrides all other signals
                }
            }

            var signals = new List<BoundarySignal>();
            double totalWeight = 0;
            double supportingWeight = 0;

            // CRITICAL: Check for page header/footer changes - strongest signal for document boundaries
            // Same header/footer across pages = same document
            // Different header/footer = likely new document
            if (!string.IsNullOrWhiteSpace(currentPage.PageHeader) && 
                !string.IsNullOrWhiteSpace(previousPage.PageHeader))
            {
                if (currentPage.PageHeader != previousPage.PageHeader)
                {
                    signals.Add(new BoundarySignal
                    {
                        SignalType = BoundarySignalType.EntityChange,
                        Description = $"Page header changed: '{previousPage.PageHeader}' → '{currentPage.PageHeader}'",
                        Weight = 0.85,
                        SupportsNewBoundary = true
                    });
                    supportingWeight += 0.85;
                    totalWeight += 0.85;
                }
            }

            if (!string.IsNullOrWhiteSpace(currentPage.PageFooter) && 
                !string.IsNullOrWhiteSpace(previousPage.PageFooter))
            {
                if (currentPage.PageFooter != previousPage.PageFooter)
                {
                    signals.Add(new BoundarySignal
                    {
                        SignalType = BoundarySignalType.EntityChange,
                        Description = $"Page footer changed: '{previousPage.PageFooter}' → '{currentPage.PageFooter}'",
                        Weight = 0.8,
                        SupportsNewBoundary = true
                    });
                    supportingWeight += 0.8;
                    totalWeight += 0.8;
                }
            }

            // Check for page number resets - "Page 1 of X" indicates new document start
            if (!string.IsNullOrWhiteSpace(currentPage.PageNumberText) ||
                !string.IsNullOrWhiteSpace(currentPage.PageFooter))
            {
                var pageNumText = (currentPage.PageNumberText ?? currentPage.PageFooter ?? "").ToLowerInvariant();
                
                // "Page 1 of X" with DIFFERENT total than previous = NEW document sequence
                if (currentPageNum.HasValue && currentPageNum.Value.current == 1)
                {
                    bool isNewSequence = !previousPageNum.HasValue || 
                                        previousPageNum.Value.total != currentPageNum.Value.total;
                    
                    if (isNewSequence)
                    {
                        signals.Add(new BoundarySignal
                        {
                            SignalType = BoundarySignalType.FormReference,
                            Description = $"New document sequence: Page 1 of {currentPageNum.Value.total}" + 
                                         (previousPageNum.HasValue ? $" (previous was 1 of {previousPageNum.Value.total})" : ""),
                            Weight = 0.95,  // Very strong signal
                            SupportsNewBoundary = true
                        });
                        supportingWeight += 0.95;
                        totalWeight += 0.95;
                    }
                }
            }

            // Check for title/heading roles in first paragraphs
            var firstParagraphs = currentPage.Paragraphs.Take(3).ToList();
            foreach (var para in firstParagraphs)
            {
                if (para.Role == "title" || para.Role == "sectionHeading")
                {
                    var lowerContent = para.Content.ToLowerInvariant();
                    var isMajorBoundary = MajorBoundaryKeywords.Any(k => lowerContent.Contains(k));
                    var isSectionBoundary = SectionKeywords.Any(k => lowerContent.Contains(k));

                    if (isMajorBoundary)
                    {
                        signals.Add(new BoundarySignal
                        {
                            SignalType = BoundarySignalType.StrongTitle,
                            Description = $"Major document heading: '{para.Content}'",
                            Weight = 0.9,
                            SupportsNewBoundary = true
                        });
                        supportingWeight += 0.9;
                        totalWeight += 0.9;
                    }
                    else if (isSectionBoundary)
                    {
                        signals.Add(new BoundarySignal
                        {
                            SignalType = BoundarySignalType.StrongTitle,
                            Description = $"Section heading: '{para.Content}'",
                            Weight = 0.6,
                            SupportsNewBoundary = true
                        });
                        supportingWeight += 0.6;
                        totalWeight += 0.6;
                    }
                    else if (para.Role == "title")
                    {
                        signals.Add(new BoundarySignal
                        {
                            SignalType = BoundarySignalType.StrongTitle,
                            Description = $"Title detected: '{para.Content}'",
                            Weight = 0.5,
                            SupportsNewBoundary = true
                        });
                        supportingWeight += 0.5;
                        totalWeight += 0.5;
                    }
                }
            }

            // Check for large tables (often indicate new sections/documents)
            if (currentPage.Tables.Any(t => t.RowCount > 5))
            {
                signals.Add(new BoundarySignal
                {
                    SignalType = BoundarySignalType.LayoutChange,
                    Description = "Large table detected (potential contract terms/conditions)",
                    Weight = 0.4,
                    SupportsNewBoundary = true
                });
                supportingWeight += 0.4;
                totalWeight += 0.4;
            }

            // Check for page number resets or "Page 1 of N" patterns
            var pageText = string.Join(" ", currentPage.Paragraphs.Select(p => p.Content));
            if (System.Text.RegularExpressions.Regex.IsMatch(pageText, @"page\s+1\s+of\s+\d+", 
                System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            {
                signals.Add(new BoundarySignal
                {
                    SignalType = BoundarySignalType.FormReference,
                    Description = "Page numbering reset detected (Page 1 of N)",
                    Weight = 0.7,
                    SupportsNewBoundary = true
                });
                supportingWeight += 0.7;
                totalWeight += 0.7;
            }

            // Check for signature pages followed by new content
            var previousHasSignature = previousPage.Paragraphs.Any(p => 
                p.Content.ToLowerInvariant().Contains("signature") ||
                p.Content.ToLowerInvariant().Contains("signed") ||
                p.Content.ToLowerInvariant().Contains("executed"));

            var currentHasContent = currentPage.Paragraphs.Count > 3;

            if (previousHasSignature && currentHasContent)
            {
                signals.Add(new BoundarySignal
                {
                    SignalType = BoundarySignalType.SignaturePage,
                    Description = "Signature page transition",
                    Weight = 0.75,
                    SupportsNewBoundary = true
                });
                supportingWeight += 0.75;
                totalWeight += 0.75;
            }

            // If no signals found, not a boundary
            if (signals.Count == 0)
            {
                return null;
            }

            // Calculate confidence
            var confidence = totalWeight > 0 ? supportingWeight / totalWeight : 0;

            // Infer document type from signals
            var documentType = InferDocumentType(signals, currentPage);

            return new DocumentBoundary
            {
                PageNumber = pageNumber,
                ConfidenceScore = confidence,
                DocumentType = documentType,
                DocumentTypeConfidence = confidence,
                BoundarySignals = signals
            };
        }

        private string InferDocumentType(List<BoundarySignal> signals, PageAnalysis page)
        {
            var allText = string.Join(" ", signals.Select(s => s.Description)) + " " +
                          string.Join(" ", page.Paragraphs.Select(p => p.Content));
            
            var lowerText = allText.ToLowerInvariant();

            if (lowerText.Contains("closing disclosure")) return "ClosingDisclosure";
            if (lowerText.Contains("loan estimate")) return "LoanEstimate";
            if (lowerText.Contains("promissory note") || lowerText.Contains("note")) return "PromissoryNote";
            if (lowerText.Contains("deed of trust")) return "DeedOfTrust";
            if (lowerText.Contains("mortgage")) return "Mortgage";
            if (lowerText.Contains("form 1003")) return "LoanApplication1003";
            if (lowerText.Contains("purchase agreement")) return "PurchaseAgreement";
            if (lowerText.Contains("lease agreement")) return "LeaseAgreement";
            if (lowerText.Contains("settlement statement")) return "SettlementStatement";
            if (lowerText.Contains("contract") || lowerText.Contains("agreement")) return "Contract";

            return LoanDocumentTypes.Unknown;
        }
    }
}
--------------------
using DocumentProcessing.Models;
using Microsoft.Extensions.Logging;
using System.Security.Cryptography;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Service for computing SHA-256 hashes of document segments.
    /// 
    /// MVP Implementation: Computes only SHA-256 hashes.
    /// - SHA-256 hash: Computed on raw bytes for exact duplicate detection.
    ///   Same bytes always produce the same SHA-256 hash.
    /// 
    /// Note: Perceptual hashing (for visual similarity) requires image processing libraries.
    /// For future enhancement, integrate a perceptual hashing service.
    /// </summary>
    public class HashingService
    {
        private readonly ILogger<HashingService>? _logger;

        public HashingService(ILogger<HashingService>? logger = null)
        {
            _logger = logger;
        }

        /// <summary>
        /// Computes SHA-256 hash for a document segment.
        /// Perceptual hash is generated as a deterministic fallback based on byte content.
        /// </summary>
        public HashResult ComputeHashes(byte[] segmentBytes, DocumentSegment segment, string batchId)
        {
            _logger?.LogDebug("Computing hashes for segment {SegmentId} ({PageCount} pages, {ByteCount} bytes)", 
                segment.SegmentId, segment.PageCount, segmentBytes.Length);
            
            var sha256 = ComputeSha256Hash(segmentBytes);
            var perceptualHash = ComputeFallbackPerceptualHash(segmentBytes);

            _logger?.LogDebug("Hashes computed - SHA256: {Sha256}, Perceptual: {Perceptual}", 
                sha256.Substring(0, 16) + "...", perceptualHash.Substring(0, 16) + "...");

            return new HashResult
            {
                SegmentId = segment.SegmentId,
                BatchId = batchId,
                SourceFileName = segment.SourceFileName,
                BlobPath = segment.OutputBlobPath,
                Sha256Hash = sha256,
                PerceptualHash = perceptualHash,
                PageCount = segment.PageCount,
                ComputedAt = DateTime.UtcNow,
                DetectedAsDuplicate = segment.DetectedAsDuplicate,
                DuplicateOfSegmentIndex = segment.DuplicateOfSegmentIndex
            };
        }

        /// <summary>
        /// Computes SHA-256 hash of the segment bytes.
        /// </summary>
        public string ComputeSha256Hash(byte[] data)
        {
            using (var sha256 = SHA256.Create())
            {
                var hashBytes = sha256.ComputeHash(data);
                return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
            }
        }

        /// <summary>
        /// Computes a deterministic perceptual hash based on byte content.
        /// This is a simple fallback that doesn't require image processing libraries.
        /// 
        /// Algorithm:
        /// 1. Divides bytes into 64 chunks.
        /// 2. Computes average value of each chunk.
        /// 3. Compares each chunk average to overall average.
        /// 4. Returns 64-bit hash as hex string.
        /// 
        /// This provides basic similarity detection without external libraries.
        /// </summary>
        public string ComputeFallbackPerceptualHash(byte[] data)
        {
            try
            {
                // Sample data from throughout the document to avoid PDF header bias
                // Take samples from beginning, middle, and end
                var sampleSize = Math.Min(4096, data.Length); // Increased from 1KB to 4KB
                var sample = new List<byte>();
                
                // Beginning (skip first 512 bytes which are PDF headers)
                int skipHeader = Math.Min(512, data.Length / 10);
                int beginSize = Math.Min(1024, (data.Length - skipHeader) / 3);
                if (data.Length > skipHeader + beginSize)
                {
                    sample.AddRange(data.Skip(skipHeader).Take(beginSize));
                }
                
                // Middle
                int midStart = data.Length / 2 - 512;
                int midSize = Math.Min(1024, data.Length / 3);
                if (midStart > 0 && midStart + midSize < data.Length)
                {
                    sample.AddRange(data.Skip(midStart).Take(midSize));
                }
                
                // End (skip last 128 bytes which are PDF EOF markers)
                int endSize = Math.Min(1024, data.Length / 3);
                int endStart = Math.Max(0, data.Length - endSize - 128);
                if (endStart < data.Length && endSize > 0)
                {
                    sample.AddRange(data.Skip(endStart).Take(Math.Min(endSize, data.Length - endStart)));
                }
                
                var sampleArray = sample.Take(sampleSize).ToArray();
                if (sampleArray.Length == 0)
                {
                    return data.Length.ToString("x16") + "00000000";
                }
                
                // Compute overall average
                double overallAverage = sampleArray.Average(b => (double)b);
                
                // Divide into 64 chunks and compute average for each
                const int hashBits = 64;
                var chunkSize = Math.Max(1, sampleArray.Length / hashBits);
                var hashBits_arr = new bool[hashBits];
                
                for (int i = 0; i < hashBits && i * chunkSize < sampleArray.Length; i++)
                {
                    var chunkStart = i * chunkSize;
                    var chunkEnd = Math.Min(chunkStart + chunkSize, sampleArray.Length);
                    var chunk = sampleArray[chunkStart..chunkEnd];
                    
                    double chunkAverage = chunk.Length > 0 ? chunk.Average(b => (double)b) : 0;
                    hashBits_arr[i] = chunkAverage > overallAverage;
                }
                
                // Convert boolean array to hex string
                var result = "";
                for (int i = 0; i < hashBits; i += 8)
                {
                    byte b = 0;
                    for (int j = 0; j < 8 && i + j < hashBits; j++)
                    {
                        if (hashBits_arr[i + j])
                        {
                            b |= (byte)(1 << j);
                        }
                    }
                    result += b.ToString("x2");
                }
                
                return result;
            }
            catch
            {
                // Fallback: hash based on length and first/last bytes
                var lengthHash = data.Length.ToString("x16");
                var contentHash = data.Length > 0 
                    ? $"{data[0]:x2}{data[data.Length - 1]:x2}" 
                    : "0000";
                return lengthHash + contentHash;
            }
        }
    }
}
---------------------
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using System.Net.Http.Headers;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Service for syncing final processed documents to SharePoint for governance and user access.
    /// This is a secondary storage layer - all processing happens in Blob Storage.
    /// SIMPLIFIED VERSION: Uses HTTP client for file uploads to avoid Graph SDK complexities.
    /// </summary>
    public class SharePointSyncService : IDisposable
    {
        // ✅ FIX #7: Use traditional array initialization (compatible with all C# versions)
        private static readonly char[] InvalidFileNameChars = new[] { '~', '"', '#', '%', '&', '*', ':', '<', '>', '?', '/', '\\', '{', '|', '}' };
        private static readonly char[] InvalidPathChars = new[] { '~', '"', '#', '%', '&', '*', ':', '<', '>', '?', '\\', '{', '|', '}' };

        private readonly HttpClient _httpClient;
        private readonly string? _accessToken;
        private readonly string? _siteId;
        private readonly string? _driveId;
        private readonly string? _libraryName;
        private readonly ILogger<SharePointSyncService> _logger;
        private bool _isEnabled;
        private bool _disposed = false;

        public SharePointSyncService(
            string? tenantId,
            string? clientId,
            string? clientSecret,
            string? siteUrl,
            string? libraryName,
            ILogger<SharePointSyncService> logger)
        {
            _logger = logger;
            _libraryName = libraryName;
            _httpClient = new HttpClient();
            _httpClient.Timeout = TimeSpan.FromMinutes(5); // Set reasonable timeout

            // Check if SharePoint is configured
            _isEnabled = !string.IsNullOrWhiteSpace(tenantId)
                && !string.IsNullOrWhiteSpace(clientId)
                && !string.IsNullOrWhiteSpace(clientSecret)
                && !string.IsNullOrWhiteSpace(siteUrl);

            if (_isEnabled)
            {
                try
                {
                    _logger.LogInformation("Initializing SharePoint sync service...");
                    _logger.LogInformation("TenantId: {TenantId}, ClientId: {ClientId}, SiteUrl: {SiteUrl}, LibraryName: {LibraryName}",
                        tenantId, clientId, siteUrl, libraryName);

                    // ✅ FIX #1: Use async initialization pattern with ConfigureAwait(false) to prevent deadlocks
                    var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);

                    // Get access token synchronously but safely
                    _logger.LogDebug("Acquiring access token...");
                    var tokenTask = credential.GetTokenAsync(
                        new Azure.Core.TokenRequestContext(new[] { "https://graph.microsoft.com/.default" }),
                        default);

                    // Use ConfigureAwait(false) and GetAwaiter().GetResult() instead of .Result or .Wait()
                    var token = tokenTask.ConfigureAwait(false).GetAwaiter().GetResult();
                    _accessToken = token.Token;
                    _logger.LogInformation("Access token acquired successfully");

                    // Initialize Graph client
                    var graphClient = new GraphServiceClient(credential);

                    // Get site and drive IDs
                    var uri = new Uri(siteUrl!);
                    var hostname = uri.Host;
                    var sitePath = uri.AbsolutePath;
                    _logger.LogDebug("Resolving site: {Hostname}:{SitePath}", hostname, sitePath);

                    // ✅ FIX #1: Use ConfigureAwait(false) pattern
                    var siteTask = graphClient.Sites[$"{hostname}:{sitePath}"].GetAsync();
                    var site = siteTask.ConfigureAwait(false).GetAwaiter().GetResult();
                    _siteId = site?.Id;
                    _logger.LogInformation("Site resolved: {SiteId}", _siteId);

                    if (string.IsNullOrEmpty(_siteId))
                    {
                        throw new InvalidOperationException("Failed to resolve SharePoint site ID");
                    }

                    // ✅ FIX: Get drive for specific library, not default site drive
                    // If libraryName is specified, get that library's drive
                    if (!string.IsNullOrWhiteSpace(_libraryName))
                    {
                        _logger.LogDebug("Looking for drive with name: {LibraryName}", _libraryName);
                        var drivesTask = graphClient.Sites[_siteId].Drives.GetAsync();
                        var drivesResponse = drivesTask.ConfigureAwait(false).GetAwaiter().GetResult();
                        var drives = drivesResponse?.Value;

                        if (drives != null)
                        {
                            // Find the drive that matches the library name
                            var targetDrive = drives.FirstOrDefault(d =>
                                d.Name?.Equals(_libraryName, StringComparison.OrdinalIgnoreCase) == true);

                            if (targetDrive != null)
                            {
                                _driveId = targetDrive.Id;
                                _logger.LogInformation("Found library drive: {LibraryName} → {DriveId}", _libraryName, _driveId);
                            }
                            else
                            {
                                _logger.LogWarning("Library '{LibraryName}' not found. Available drives: {Drives}",
                                    _libraryName, string.Join(", ", drives.Select(d => d.Name)));
                                // Fall back to default drive
                                var driveTask = graphClient.Sites[_siteId].Drive.GetAsync();
                                var drive = driveTask.ConfigureAwait(false).GetAwaiter().GetResult();
                                _driveId = drive?.Id;
                                _logger.LogWarning("Using default drive: {DriveId}", _driveId);
                            }
                        }
                    }
                    else
                    {
                        // No library name specified, use default drive
                        var driveTask = graphClient.Sites[_siteId].Drive.GetAsync();
                        var drive = driveTask.ConfigureAwait(false).GetAwaiter().GetResult();
                        _driveId = drive?.Id;
                        _logger.LogInformation("Drive resolved (default): {DriveId}", _driveId);
                    }

                    if (string.IsNullOrEmpty(_driveId))
                    {
                        throw new InvalidOperationException("Failed to resolve SharePoint drive ID");
                    }

                    _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);

                    _logger.LogInformation("✅ SharePoint sync service ENABLED and ready. Site: {SiteId}, Drive: {DriveId}", _siteId, _driveId);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "❌ Failed to initialize SharePoint sync service. Syncing will be disabled. Error: {ErrorMessage}", ex.Message);
                    _isEnabled = false;
                }
            }
            else
            {
                _logger.LogWarning("⚠️ SharePoint sync service DISABLED (missing configuration). TenantId={HasTenantId}, ClientId={HasClientId}, ClientSecret={HasSecret}, SiteUrl={HasSiteUrl}",
                    !string.IsNullOrWhiteSpace(tenantId), !string.IsNullOrWhiteSpace(clientId), !string.IsNullOrWhiteSpace(clientSecret), !string.IsNullOrWhiteSpace(siteUrl));
            }
        }

        /// <summary>
        /// Uploads a document to SharePoint (best-effort, non-blocking).
        /// Returns true if successful, false if failed or disabled.
        /// </summary>
        public async Task<bool> UploadDocumentAsync(string folderPath, string fileName, byte[] content)
        {
            _logger.LogDebug("📤 UploadDocumentAsync called. Enabled={IsEnabled}, SiteId={SiteId}, DriveId={DriveId}, Folder={Folder}, File={File}, Size={Size} bytes",
                _isEnabled, _siteId ?? "null", _driveId ?? "null", folderPath, fileName, content?.Length ?? 0);
            
            if (content == null)
            {
                _logger.LogWarning("⚠️ SharePoint sync skipped. Content is null.");
                return false;
            }
            
            if (!_isEnabled || string.IsNullOrEmpty(_siteId) || string.IsNullOrEmpty(_driveId))
            {
                _logger.LogWarning("⚠️ SharePoint sync skipped. Enabled={IsEnabled}, HasSiteId={HasSiteId}, HasDriveId={HasDriveId}",
                    _isEnabled, !string.IsNullOrEmpty(_siteId), !string.IsNullOrEmpty(_driveId));
                return false;
            }

            try
            {
                var sanitizedFileName = SanitizeFileName(fileName);
                var sanitizedFolderPath = SanitizeFolderPath(folderPath);
                _logger.LogDebug("Sanitized: Folder='{SanitizedFolder}', File='{SanitizedFile}'", sanitizedFolderPath, sanitizedFileName);

                // ✅ FIX: Build path WITHOUT library name (we're already in the correct drive)
                var pathParts = new List<string>();
                // Don't add library name - we already selected the correct drive
                if (!string.IsNullOrEmpty(sanitizedFolderPath))
                    pathParts.Add(sanitizedFolderPath);
                pathParts.Add(sanitizedFileName);

                var uploadPath = string.Join("/", pathParts);
                _logger.LogDebug("Path parts: Library={Library}, Folder={Folder}, File={File}", _libraryName, sanitizedFolderPath, sanitizedFileName);
                _logger.LogInformation("📤 Starting upload to SharePoint path: {UploadPath}", uploadPath);

                // ✅ FIX #5: URL encode the path to handle special characters
                var encodedPath = Uri.EscapeDataString(uploadPath).Replace("%2F", "/");

                // ✅ FIX #6: Use specific library drive ID, not default site drive
                var url = $"https://graph.microsoft.com/v1.0/sites/{_siteId}/drives/{_driveId}/root:/{encodedPath}:/content";
                _logger.LogDebug("Full URL: {Url}", url);
                
                using var contentStream = new ByteArrayContent(content);
                contentStream.Headers.ContentType = new MediaTypeHeaderValue("application/pdf");
                
                _logger.LogDebug("Sending PUT request to Graph API...");
                var response = await _httpClient.PutAsync(url, contentStream);
                _logger.LogDebug("Response received: Status={StatusCode}, Reason={ReasonPhrase}", 
                    response.StatusCode, response.ReasonPhrase);
                
                if (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                {
                    _logger.LogWarning("⚠️ SharePoint throttling (429) detected. Retrying after 5 seconds...");
                    await Task.Delay(5000);
                    
                    // Recreate content stream for retry
                    using var retryContentStream = new ByteArrayContent(content);
                    retryContentStream.Headers.ContentType = new MediaTypeHeaderValue("application/pdf");
                    response = await _httpClient.PutAsync(url, retryContentStream);
                    _logger.LogDebug("Retry response: Status={StatusCode}, Reason={ReasonPhrase}", 
                        response.StatusCode, response.ReasonPhrase);
                }
                
                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation("✅ Upload completed successfully: {Path}", uploadPath);
                    return true;
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    _logger.LogError("❌ SharePoint upload FAILED. Status={StatusCode}, Reason={ReasonPhrase}, Path={Path}, Error={Error}", 
                        response.StatusCode, response.ReasonPhrase, uploadPath, errorContent);
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Exception during SharePoint upload: {FileName}. Error: {ErrorMessage}", fileName, ex.Message);
                return false;
            }
        }

        private static string SanitizeFileName(string fileName)
        {
            // SharePoint invalid characters: ~ " # % & * : < > ? / \\ { | }
            foreach (var c in InvalidFileNameChars)
            {
                fileName = fileName.Replace(c, '_');
            }

            // Limit length to 128 characters (SharePoint safe limit)
            if (fileName.Length > 128)
            {
                var extension = Path.GetExtension(fileName);
                var nameWithoutExt = Path.GetFileNameWithoutExtension(fileName);
                fileName = string.Concat(nameWithoutExt.AsSpan(0, 128 - extension.Length), extension);
            }

            return fileName;
        }

        private static string SanitizeFolderPath(string folderPath)
        {
            // Remove leading/trailing slashes
            folderPath = folderPath.Trim('/');
            
            // Replace invalid characters
            foreach (var c in InvalidPathChars)
            {
                folderPath = folderPath.Replace(c, '_');
            }

            return folderPath;
        }

        public bool IsEnabled => _isEnabled;

        // ✅ FIX #4: Implement IDisposable to properly dispose HttpClient
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _httpClient?.Dispose();
                    _logger?.LogDebug("SharePoint sync service disposed");
                }
                _disposed = true;
            }
        }
    }
}
----------------
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using DocumentProcessing.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace DocumentProcessing.Services
{
    /// <summary>
    /// Service for interacting with Azure Storage (Blobs and Tables).
    /// Handles reading PDFs, storing split segments, and managing the document index.
    /// </summary>
    public class StorageService
    {
        private readonly BlobContainerClient _incomingContainer;
        private readonly BlobContainerClient _splitContainer;
        private readonly BlobContainerClient _resultsContainer;
        private readonly BlobContainerClient _duplicatesContainer;
        private readonly TableClient _indexTableClient;
        private readonly ILogger<StorageService>? _logger;

        public StorageService(string storageConnectionString, 
            string incomingContainerName, 
            string splitContainerName, 
            string resultsContainerName,
            string duplicatesContainerName,
            string indexTableName,
            ILogger<StorageService>? logger = null)
        {
            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            _incomingContainer = blobServiceClient.GetBlobContainerClient(incomingContainerName);
            _splitContainer = blobServiceClient.GetBlobContainerClient(splitContainerName);
            _resultsContainer = blobServiceClient.GetBlobContainerClient(resultsContainerName);
            _duplicatesContainer = blobServiceClient.GetBlobContainerClient(duplicatesContainerName);

            var tableServiceClient = new TableServiceClient(storageConnectionString);
            _indexTableClient = tableServiceClient.GetTableClient(indexTableName);
            _logger = logger;
        }

        /// <summary>
        /// Reads a PDF from the incoming container.
        /// </summary>
        public async Task<byte[]> ReadIncomingPdfAsync(string blobName)
        {
            _logger?.LogDebug("Reading PDF from incoming container: {BlobName}", blobName);
            var blobClient = _incomingContainer.GetBlobClient(blobName);
            var download = await blobClient.DownloadAsync();
            using (var memStream = new MemoryStream())
            {
                await download.Value.Content.CopyToAsync(memStream);
                var bytes = memStream.ToArray();
                _logger?.LogInformation("Read {ByteCount} bytes from {BlobName}", bytes.Length, blobName);
                return bytes;
            }
        }

        /// <summary>
        /// Opens a stream to a PDF in the incoming container without loading the entire file into memory.
        /// Use this for checking page counts on large files to minimize memory footprint.
        /// </summary>
        public async Task<Stream> OpenIncomingPdfStreamAsync(string blobName)
        {
            _logger?.LogDebug("Opening stream for PDF in incoming container: {BlobName}", blobName);
            var blobClient = _incomingContainer.GetBlobClient(blobName);
            return await blobClient.OpenReadAsync();
        }

        /// <summary>
        /// Reads a segment PDF from the split container by its blob path.
        /// Returns null if the segment is not found.
        /// </summary>
        public async Task<byte[]?> ReadSegmentAsync(string blobPath)
        {
            try
            {
                var blobClient = _splitContainer.GetBlobClient(blobPath);
                var download = await blobClient.DownloadAsync();
                using (var memStream = new MemoryStream())
                {
                    await download.Value.Content.CopyToAsync(memStream);
                    return memStream.ToArray();
                }
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 404)
            {
                // Segment not found
                return null;
            }
        }

        /// <summary>
        /// Stores a split document segment to the split container.
        /// </summary>
        public async Task<string> StoreSplitSegmentAsync(string blobPath, byte[] pdfBytes)
        {
            if (pdfBytes == null || pdfBytes.Length == 0)
                throw new InvalidOperationException($"Cannot store empty segment: {blobPath}. Segment bytes are null or empty.");
            
            var blobClient = _splitContainer.GetBlobClient(blobPath);
            await blobClient.UploadAsync(new MemoryStream(pdfBytes), overwrite: true);
            return blobClient.Uri.ToString();
        }

        /// <summary>
        /// Stores a hash result to the index table for future deduplication lookups.
        /// </summary>
        public async Task StoreHashResultAsync(HashResult hashResult)
        {
            var tableEntity = new TableEntity(
                partitionKey: "HashIndex",
                rowKey: hashResult.SegmentId
            )
            {
                { "BlobPath", hashResult.BlobPath },
                { "Sha256Hash", hashResult.Sha256Hash },
                { "PerceptualHash", hashResult.PerceptualHash },
                { "PageCount", hashResult.PageCount },
                { "ComputedAt", hashResult.ComputedAt }
            };

            await _indexTableClient.UpsertEntityAsync(tableEntity);
        }

        /// <summary>
        /// Stores a deduplication result to the index table.
        /// </summary>
        public async Task StoreDeduplicationResultAsync(DeduplicationResult dedupResult)
        {
            var tableEntity = new TableEntity(
                partitionKey: "DeduplicationResults",
                rowKey: dedupResult.RecordId
            )
            {
                { "SegmentId", dedupResult.SegmentId },
                { "BlobPath", dedupResult.BlobPath },
                { "Sha256Hash", dedupResult.Sha256Hash },
                { "PerceptualHash", dedupResult.PerceptualHash },
                { "IsExactDuplicate", dedupResult.IsExactDuplicate },
                { "ExactDuplicateOf", dedupResult.ExactDuplicateOf ?? "" },
                { "IsSimilar", dedupResult.IsSimilar },
                { "SimilarSegmentIds", JsonSerializer.Serialize(dedupResult.SimilarSegmentIds) },
                { "MinHammingDistance", dedupResult.MinHammingDistance },
                { "Status", dedupResult.Status },
                { "ProcessedAt", dedupResult.ProcessedAt }
            };

            await _indexTableClient.UpsertEntityAsync(tableEntity);
        }

        /// <summary>
        /// Loads all hash results from the index table to rebuild the deduplication index.
        /// </summary>
        public async Task<Dictionary<string, HashResult>> LoadHashIndexAsync()
        {
            var index = new Dictionary<string, HashResult>();

            try
            {
                var query = _indexTableClient.QueryAsync<TableEntity>(
                    e => e.PartitionKey == "HashIndex"
                );

                await foreach (var entity in query)
                {
                    var hashResult = new HashResult
                    {
                        SegmentId = entity.RowKey,
                        BlobPath = entity.GetString("BlobPath"),
                        Sha256Hash = entity.GetString("Sha256Hash"),
                        PerceptualHash = entity.GetString("PerceptualHash"),
                        PageCount = entity.GetInt32("PageCount") ?? 0,
                        ComputedAt = entity.GetDateTime("ComputedAt") ?? DateTime.UtcNow
                    };
                    index[entity.RowKey] = hashResult;
                }
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 404)
            {
                // Table doesn't exist yet; return empty index.
            }

            return index;
        }

        /// <summary>
        /// Writes a processing summary to the results container.
        /// </summary>
        public async Task WriteResultsSummaryAsync(string batchId, string summary)
        {
            var resultsFileName = $"batch-{batchId}-summary.json";
            var blobClient = _resultsContainer.GetBlobClient(resultsFileName);
            await blobClient.UploadAsync(new MemoryStream(System.Text.Encoding.UTF8.GetBytes(summary)), overwrite: true);
        }

        /// <summary>
        /// Stores a duplicate segment to the duplicates container.
        /// Organizes by segment ID or exact duplicate reference for easy tracking.
        /// </summary>
        public async Task StoreDuplicateSegmentAsync(string blobPath, byte[] segmentPdfBytes)
        {
            // Preserve the segment structure: duplicates/{segmentId}/{blobName}
            var blobClient = _duplicatesContainer.GetBlobClient(blobPath);
            await blobClient.UploadAsync(new MemoryStream(segmentPdfBytes), overwrite: true);
        }

        /// <summary>
        /// Reads a duplicate segment from the duplicates container.
        /// Returns null if not found.
        /// </summary>
        public async Task<byte[]?> ReadDuplicateSegmentAsync(string blobPath)
        {
            try
            {
                var blobClient = _duplicatesContainer.GetBlobClient(blobPath);
                var download = await blobClient.DownloadAsync();
                using (var memStream = new MemoryStream())
                {
                    await download.Value.Content.CopyToAsync(memStream);
                    return memStream.ToArray();
                }
            }
            catch (Azure.RequestFailedException ex) when (ex.Status == 404)
            {
                _logger?.LogWarning("Duplicate segment not found: {BlobPath}", blobPath);
                return null;
            }
        }

        /// <summary>
        /// Ensures all required containers exist.
        /// </summary>
        public async Task EnsureContainersExistAsync()
        {
            await _incomingContainer.CreateIfNotExistsAsync();
            await _splitContainer.CreateIfNotExistsAsync();
            await _resultsContainer.CreateIfNotExistsAsync();
            await _duplicatesContainer.CreateIfNotExistsAsync();
        }

        /// <summary>
        /// Ensures the index table exists.
        /// </summary>
        public async Task EnsureTableExistsAsync()
        {
            await _indexTableClient.CreateIfNotExistsAsync();
        }
    }
}
