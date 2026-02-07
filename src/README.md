using DocumentProcessing.Orchestrations;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Functions
{
    /// <summary>
    /// Blob-triggered function that automatically starts processing when a PDF is uploaded
    /// to the incoming-loan-packets container.
    /// 
    /// When a new blob is uploaded to "incoming-loan-packets/{blobName}", this function triggers
    /// and starts a new orchestration for that document.
    /// 
    /// DISABLED: Function attribute commented out to prevent automatic triggering.
    /// Use HTTP trigger (StartDocumentProcessingFunction) for manual processing instead.
    /// To re-enable, uncomment the [Function] attribute below.
    /// </summary>
    public class BlobTriggerFunction
    {
        private readonly ILogger<BlobTriggerFunction> _logger;

        public BlobTriggerFunction(ILogger<BlobTriggerFunction> logger)
        {
            _logger = logger;
        }

        // [Function(nameof(BlobTriggerFunction))]  // DISABLED - uncomment to re-enable automatic blob processing
        public async Task Run(
            [BlobTrigger("incoming-loan-packets/{name}")] Stream stream,
            string name,
            [DurableClient] DurableTaskClient client)
        {
            try
            {
                _logger.LogInformation($"Blob trigger activated for {name}");

                // Create a batch ID based on the blob name and timestamp.
                var batchId = $"{Path.GetFileNameWithoutExtension(name)}-{DateTime.UtcNow:yyyyMMddHHmmss}";

                // Start the orchestration.
                var request = new ProcessingRequest
                {
                    BatchId = batchId,
                    BlobName = name
                };

                var instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                    nameof(DocumentProcessingOrchestrator),
                    request
                );

                _logger.LogInformation(
                    $"Started orchestration {instanceId} for blob {name} (batch {batchId})"
                );
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error in blob trigger: {ex.Message}");
                throw;
            }
        }
    }
}
--------
using DocumentProcessing.Orchestrations;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using System.Net;

namespace DocumentProcessing.Functions
{
    /// <summary>
    /// HTTP-triggered function that starts the document processing orchestration.
    /// 
    /// Call this function to initiate processing of a PDF:
    /// POST /api/start-document-processing
    /// {
    ///     "batchId": "batch-001",
    ///     "blobName": "my-document.pdf"
    /// }
    /// 
    /// Response includes:
    /// - instanceId: ID of the orchestration instance (use to check status).
    /// - statusQueryGetUri: URL to check orchestration status.
    /// </summary>
    public class StartDocumentProcessingFunction
    {
        private readonly ILogger<StartDocumentProcessingFunction> _logger;

        public StartDocumentProcessingFunction(ILogger<StartDocumentProcessingFunction> logger)
        {
            _logger = logger;
        }

        [Function(nameof(StartDocumentProcessingFunction))]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "start-document-processing")] 
            HttpRequestData req,
            [DurableClient] DurableTaskClient client)
        {
            try
            {
                // Parse request body.
                var requestBody = await req.ReadAsStringAsync();
                var request = System.Text.Json.JsonSerializer.Deserialize<ProcessingRequest>(requestBody);

                if (string.IsNullOrEmpty(request?.BatchId) || string.IsNullOrEmpty(request?.BlobName))
                {
                    var badResponse = req.CreateResponse(HttpStatusCode.BadRequest);
                    await badResponse.WriteAsJsonAsync(new { error = "Missing batchId or blobName" });
                    return badResponse;
                }

                _logger.LogInformation($"Starting orchestration for batch {request.BatchId}, blob {request.BlobName}");

                // Start the orchestration.
                var instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                    nameof(DocumentProcessingOrchestrator),
                    request
                );

                // Return response with orchestration details.
                var response = req.CreateResponse(HttpStatusCode.Accepted);
                
                await response.WriteAsJsonAsync(new
                {
                    instanceId,
                    statusQueryGetUri = $"/runtime/webhooks/durableTask/instances/{instanceId}",
                    batchId = request.BatchId
                });

                return response;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error starting orchestration: {ex.Message}");
                var errorResponse = req.CreateResponse(HttpStatusCode.InternalServerError);
                await errorResponse.WriteAsJsonAsync(new { error = ex.Message });
                return errorResponse;
            }
        }
    }
}
---------------------
namespace DocumentProcessing.Models
{
    /// <summary>
    /// Result of deduplication analysis for a document segment.
    /// Indicates whether the segment is unique, an exact duplicate, or similar to existing documents.
    /// </summary>
    public class DeduplicationResult
    {
        /// <summary>
        /// Unique identifier for this deduplication record.
        /// </summary>
        public string RecordId { get; set; }

        /// <summary>
        /// Segment ID being analyzed.
        /// </summary>
        public string SegmentId { get; set; }

        /// <summary>
        /// Blob path of the segment.
        /// </summary>
        public string BlobPath { get; set; }

        /// <summary>
        /// SHA-256 hash of the segment.
        /// </summary>
        public string Sha256Hash { get; set; }

        /// <summary>
        /// Perceptual hash of the segment.
        /// </summary>
        public string PerceptualHash { get; set; }

        /// <summary>
        /// True if an existing document with the exact same SHA-256 hash was found.
        /// </summary>
        public bool IsExactDuplicate { get; set; }

        /// <summary>
        /// Segment ID of the original document if this is an exact duplicate.
        /// </summary>
        public string ExactDuplicateOf { get; set; }

        /// <summary>
        /// True if similar documents (based on perceptual hash) were found.
        /// </summary>
        public bool IsSimilar { get; set; }

        /// <summary>
        /// List of segment IDs that are similar to this segment (Hamming distance below threshold).
        /// </summary>
        public List<string> SimilarSegmentIds { get; set; } = new();

        /// <summary>
        /// Minimum Hamming distance to any similar segment (informational).
        /// </summary>
        public int MinHammingDistance { get; set; }

        /// <summary>
        /// Deduplication status: "Unique", "ExactDuplicate", or "Similar".
        /// </summary>
        public string Status { get; set; }

        /// <summary>
        /// Timestamp when deduplication was performed.
        /// </summary>
        public DateTime ProcessedAt { get; set; }
    }
}
----------
namespace DocumentProcessing.Models
{
    /// <summary>
    /// Represents a detected boundary between documents with confidence information.
    /// A boundary indicates the start of a new document within a larger package.
    /// </summary>
    public class DocumentBoundary
    {
        /// <summary>
        /// Page number (0-indexed) where this boundary occurs.
        /// The boundary page is the START of a new document.
        /// </summary>
        public int PageNumber { get; set; }

        /// <summary>
        /// Confidence score (0.0 to 1.0) that this is a true document boundary.
        /// Higher values indicate stronger evidence of a new document.
        /// </summary>
        public double ConfidenceScore { get; set; }

        /// <summary>
        /// Inferred document type for the segment starting at this boundary.
        /// Examples: "Closing Disclosure", "Promissory Note", "Deed of Trust", "Loan Application (1003)"
        /// </summary>
        public string? DocumentType { get; set; }

        /// <summary>
        /// Confidence in the document type classification (0.0 to 1.0).
        /// </summary>
        public double DocumentTypeConfidence { get; set; }

        /// <summary>
        /// List of signals that contributed to detecting this boundary.
        /// Useful for debugging and understanding why a boundary was detected.
        /// </summary>
        public List<BoundarySignal> BoundarySignals { get; set; } = new();

        /// <summary>
        /// Key entities found on this page (borrower name, loan number, property address, etc.)
        /// </summary>
        public PageEntities? PageEntities { get; set; }

        /// <summary>
        /// Whether this boundary marks the start of a detected duplicate document.
        /// Set when the boundary detector finds sequence restart with matching content hash.
        /// </summary>
        public bool IsDuplicateBoundary { get; set; } = false;

        /// <summary>
        /// If IsDuplicateBoundary is true, this is the page number of the original document.
        /// </summary>
        public int? DuplicateOfPageNumber { get; set; }
    }

    /// <summary>
    /// Represents a signal that indicates a potential document boundary or continuation.
    /// </summary>
    public class BoundarySignal
    {
        /// <summary>
        /// Type of signal detected.
        /// </summary>
        public BoundarySignalType SignalType { get; set; }

        /// <summary>
        /// Human-readable description of the signal.
        /// Example: "Strong title match: 'Closing Disclosure'"
        /// </summary>
        public required string Description { get; set; }

        /// <summary>
        /// Weight/importance of this signal (0.0 to 1.0).
        /// Higher values indicate stronger evidence.
        /// </summary>
        public double Weight { get; set; } = 1.0;

        /// <summary>
        /// Whether this signal supports a new boundary (true) or continuation (false).
        /// </summary>
        public bool SupportsNewBoundary { get; set; }
    }

    /// <summary>
    /// Types of signals used in boundary detection.
    /// </summary>
    public enum BoundarySignalType
    {
        /// <summary>
        /// Strong title/heading on the page (e.g., "Closing Disclosure")
        /// </summary>
        StrongTitle,

        /// <summary>
        /// Known form reference (e.g., "Form 1003")
        /// </summary>
        FormReference,

        /// <summary>
        /// Signature page indicator
        /// </summary>
        SignaturePage,

        /// <summary>
        /// Change in borrower name or key entity
        /// </summary>
        EntityChange,

        /// <summary>
        /// Change in document header/footer pattern
        /// </summary>
        LayoutChange,

        /// <summary>
        /// Explicit delimiter (e.g., "End of Document")
        /// </summary>
        ExplicitDelimiter,

        /// <summary>
        /// Structural element indicating continuation (e.g., "Page 2 of 5")
        /// </summary>
        ContinuationMarker,

        /// <summary>
        /// Heuristic score indicating likely boundary
        /// </summary>
        ContentHeuristic
    }

    /// <summary>
    /// Key entities extracted from a page that help with document boundary detection.
    /// </summary>
    public class PageEntities
    {
        /// <summary>
        /// Borrower name(s) detected on the page.
        /// </summary>
        public List<string> BorrowerNames { get; set; } = new();

        /// <summary>
        /// Loan number or identifier detected on the page.
        /// </summary>
        public string? LoanNumber { get; set; }

        /// <summary>
        /// Property address detected on the page.
        /// </summary>
        public string? PropertyAddress { get; set; }

        /// <summary>
        /// Document titles or headings found on the page.
        /// </summary>
        public List<string> Titles { get; set; } = new();

        /// <summary>
        /// Known form names/references found (e.g., "Form 1003", "Closing Disclosure")
        /// </summary>
        public List<string> FormReferences { get; set; } = new();

        /// <summary>
        /// Approximate text content of the page (first 500 characters).
        /// Used for pattern matching and analysis.
        /// </summary>
        public string? TextPreview { get; set; }
    }

    /// <summary>
    /// Known loan document types and their identifying characteristics.
    /// </summary>
    public static class LoanDocumentTypes
    {
        public const string ClosingDisclosure = "Closing Disclosure";
        public const string LoanEstimate = "Loan Estimate";
        public const string PromissoryNote = "Promissory Note";
        public const string Note = "Note";
        public const string Mortgage = "Mortgage";
        public const string DeedOfTrust = "Deed of Trust";
        public const string Riders = "Riders";
        public const string Addendum = "Addendum";
        public const string HUD1Settlement = "HUD-1 Settlement Statement";
        public const string TRID = "TRID Disclosure";
        public const string UniformResidentialLoanApplication = "Uniform Residential Loan Application (1003)";
        public const string LoanApplication1003 = "Loan Application (1003)";
        public const string AltDocLoan = "Alt Doc Loan";
        public const string AppraisalReport = "Appraisal Report";
        public const string CreditReport = "Credit Report";
        public const string InsurancePolicy = "Insurance Policy";
        public const string TitlePolicy = "Title Policy";
        public const string PayoffLetters = "Payoff Letters";
        public const string GiftLetter = "Gift Letter";
        public const string EmploymentVerification = "Employment Verification";
        public const string TaxReturns = "Tax Returns";
        public const string BankStatements = "Bank Statements";
        public const string Unknown = "Unknown";

        /// <summary>
        /// Get all known document types as a list.
        /// </summary>
        public static List<string> GetAllTypes()
        {
            return typeof(LoanDocumentTypes)
                .GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
                .Where(f => f.FieldType == typeof(string))
                .Select(f => (string)f.GetValue(null)!)
                .ToList();
        }
    }
}
-------------
using static DocumentProcessing.Models.LoanDocumentTypes;

namespace DocumentProcessing.Models
{
    /// <summary>
    /// Represents a segment of a document created after splitting on black pages.
    /// Includes document type classification and confidence metrics.
    /// </summary>
    public class DocumentSegment
    {
        /// <summary>
        /// Unique identifier for this segment.
        /// </summary>
        public required string SegmentId { get; set; }

        /// <summary>
        /// Original PDF filename from which this segment was extracted.
        /// </summary>
        public required string SourceFileName { get; set; }

        /// <summary>
        /// Starting page number (1-indexed) in the original PDF.
        /// </summary>
        public int StartPageNumber { get; set; }

        /// <summary>
        /// Ending page number (1-indexed) in the original PDF.
        /// </summary>
        public int EndPageNumber { get; set; }

        /// <summary>
        /// Total number of pages in this segment.
        /// </summary>
        public int PageCount { get; set; }

        /// <summary>
        /// Blob path where this segment is stored (e.g., "split-documents/source.pdf/segment-001.pdf").
        /// </summary>
        public required string OutputBlobPath { get; set; }

        /// <summary>
        /// Size of the segment in bytes.
        /// </summary>
        public long SizeBytes { get; set; }

        /// <summary>
        /// Timestamp when this segment was created.
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Extracted PDF bytes for this segment (used during processing, not stored in table).
        /// </summary>
        public byte[]? SegmentBytes { get; set; }

        /// <summary>
        /// Inferred document type (e.g., "Closing Disclosure", "Note", "Deed of Trust").
        /// Optional - for future enhancement with text extraction.
        /// </summary>
        public string DocumentType { get; set; } = LoanDocumentTypes.Unknown;

        /// <summary>
        /// Confidence score (0.0 to 1.0) for the document type classification.
        /// Optional - for future enhancement with text extraction.
        /// </summary>
        public double DocumentTypeConfidence { get; set; } = 0.0;

        /// <summary>
        /// Confidence score (0.0 to 1.0) from DocumentBoundaryDetector for boundary detection.
        /// Indicates how confident the system is that this page marks a logical document boundary.
        /// </summary>
        public double BoundaryConfidence { get; set; } = 0.0;

        /// <summary>
        /// Whether this segment was detected as a duplicate during boundary detection.
        /// Set when the boundary detector finds sequence restart with matching content.
        /// </summary>
        public bool DetectedAsDuplicate { get; set; } = false;

        /// <summary>
        /// If DetectedAsDuplicate is true, this indicates which segment (by index) it duplicates.
        /// Zero-based index of the original segment within the same batch.
        /// </summary>
        public int? DuplicateOfSegmentIndex { get; set; }

        /// <summary>
        /// Whether this segment is an exact duplicate (SHA-256 hash match) of another segment.
        /// Set during deduplication activity.
        /// </summary>
        public bool IsExactDuplicate { get; set; } = false;

        /// <summary>
        /// Whether this segment is similar (perceptual hash match) to another segment.
        /// Set during deduplication activity.
        /// </summary>
        public bool IsSimilar { get; set; } = false;

        /// <summary>
        /// If IsExactDuplicate is true, this contains the SegmentId of the original segment.
        /// Used to construct the duplicate storage path.
        /// </summary>
        public string? ExactDuplicateOf { get; set; }

        /// <summary>
        /// Minimum Hamming distance from similar segments (for perceptual hash).
        /// Used to construct the similar duplicate storage path.
        /// </summary>
        public int MinHammingDistance { get; set; } = 0;
    }
}
----------------
namespace DocumentProcessing.Models
{
    /// <summary>
    /// Contains hash values for a document segment.
    /// Includes both cryptographic (SHA-256) and perceptual hashes for deduplication.
    /// </summary>
    public class HashResult
    {
        /// <summary>
        /// Segment ID that was hashed.
        /// </summary>
        public string SegmentId { get; set; }

        /// <summary>
        /// Batch ID that processed this segment (used to avoid comparing within same batch).
        /// </summary>
        public string BatchId { get; set; }

        /// <summary>
        /// Source file name (used to avoid comparing segments from the same document).
        /// </summary>
        public string SourceFileName { get; set; }

        /// <summary>
        /// Blob path of the segment.
        /// </summary>
        public string BlobPath { get; set; }

        /// <summary>
        /// SHA-256 hash of the segment's raw bytes (for exact duplicate detection).
        /// </summary>
        public string Sha256Hash { get; set; }

        /// <summary>
        /// Perceptual hash (average hash) of the segment's rendered image (for similarity detection).
        /// Stored as a hex string for easy comparison.
        /// </summary>
        public string PerceptualHash { get; set; }

        /// <summary>
        /// Number of pages in the segment (cached for reference).
        /// </summary>
        public int PageCount { get; set; }

        /// <summary>
        /// Timestamp when hashes were computed.
        /// </summary>
        public DateTime ComputedAt { get; set; }

        /// <summary>
        /// Whether this segment was detected as a duplicate during boundary detection.
        /// Set when the boundary detector finds sequence restart with matching content.
        /// </summary>
        public bool DetectedAsDuplicate { get; set; } = false;

        /// <summary>
        /// If DetectedAsDuplicate is true, this indicates which segment (by index) it duplicates.
        /// Zero-based index of the original segment within the same batch.
        /// </summary>
        public int? DuplicateOfSegmentIndex { get; set; }
    }
}
