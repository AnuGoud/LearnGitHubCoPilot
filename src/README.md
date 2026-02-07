using DocumentProcessing.Models;
using DocumentProcessing.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Activities
{
    /// <summary>
    /// Activity function that computes SHA-256 and perceptual hashes for document segments.
    /// 
    /// This activity processes each segment from the split result and computes:
    /// - SHA-256 hash of the segment bytes (for exact duplicate detection).
    /// - Perceptual hash (for visual similarity detection).
    /// </summary>
    public class ComputeHashesActivity
    {
        private readonly HashingService _hashingService;
        private readonly StorageService _storageService;
        private readonly ILogger<ComputeHashesActivity> _logger;

        public ComputeHashesActivity(HashingService hashingService,
            StorageService storageService,
            ILogger<ComputeHashesActivity> logger)
        {
            _hashingService = hashingService;
            _storageService = storageService;
            _logger = logger;
        }

        [Function(nameof(ComputeHashesActivity))]
        public async Task<List<HashResult>> Run([ActivityTrigger] Orchestrations.SplitResult splitResult)
        {
            try
            {
                _logger.LogInformation($"Computing hashes for {splitResult.Segments.Count} segments from batch {splitResult.BatchId}");

                var hashResults = new List<HashResult>();

                foreach (var segment in splitResult.Segments)
                {
                    // ✅ FIX: Read from correct container based on duplicate status
                    byte[]? segmentBytes;

                    if (segment.DetectedAsDuplicate)
                    {
                        // Duplicates are stored in duplicates container
                        _logger.LogDebug($"Reading duplicate segment from duplicates container: {segment.OutputBlobPath}");
                        segmentBytes = await _storageService.ReadDuplicateSegmentAsync(segment.OutputBlobPath);
                    }
                    else
                    {
                        // Unique segments are in split-documents container
                        _logger.LogDebug($"Reading unique segment from split container: {segment.OutputBlobPath}");
                        segmentBytes = await _storageService.ReadSegmentAsync(segment.OutputBlobPath);
                    }

                    if (segmentBytes == null || segmentBytes.Length == 0)
                    {
                        _logger.LogError($"Failed to read segment bytes for {segment.SegmentId} from {segment.OutputBlobPath}");
                        continue;
                    }

                    var hashResult = _hashingService.ComputeHashes(segmentBytes, segment, splitResult.BatchId);

                    // Store hash result in the index for future deduplication.
                    await _storageService.StoreHashResultAsync(hashResult);

                    hashResults.Add(hashResult);

                    _logger.LogInformation($"Computed hashes for segment {segment.SegmentId}");
                }

                return hashResults;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error computing hashes: {ex.Message}");
                throw;
            }
        }
    }
}
-----------
using DocumentProcessing.Models;
using DocumentProcessing.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Activities
{
    /// <summary>
    /// Activity function that performs deduplication analysis on hash results.
    /// 
    /// This activity:
    /// 1. Loads the existing document index from storage.
    /// 2. For each hash result, determines if it's unique, an exact duplicate, or similar.
    /// 3. Stores deduplication results back to storage.
    /// </summary>
    public class DeduplicationActivity
    {
        private readonly DeduplicationService _deduplicationService;
        private readonly StorageService _storageService;
        private readonly ILogger<DeduplicationActivity> _logger;
        private readonly BusinessMetricsLogger _metricsLogger;

        public DeduplicationActivity(DeduplicationService deduplicationService,
            StorageService storageService,
            ILogger<DeduplicationActivity> logger,
            BusinessMetricsLogger metricsLogger)
        {
            _deduplicationService = deduplicationService;
            _storageService = storageService;
            _logger = logger;
            _metricsLogger = metricsLogger;
        }

        [Function(nameof(DeduplicationActivity))]
        public async Task<List<DeduplicationResult>> Run([ActivityTrigger] List<HashResult> hashResults)
        {
            var startTime = DateTime.Now;

            try
            {
                _logger.LogInformation($"Performing deduplication on {hashResults.Count} hash results");
                _metricsLogger.LogPhaseStart("DEDUPLICATION");

                // Load existing document index from storage.
                var existingIndex = await _storageService.LoadHashIndexAsync();
                var deduplicationService = new DeduplicationService(existingIndex, _logger as ILogger<DeduplicationService>);

                var dedupResults = new List<DeduplicationResult>();

                foreach (var hashResult in hashResults)
                {
                    // Log duplicate detection flag status
                    _logger.LogInformation($"HashResult {hashResult.SegmentId}: DetectedAsDuplicate={hashResult.DetectedAsDuplicate}, DuplicateOfIndex={hashResult.DuplicateOfSegmentIndex}");

                    // Pass the full batch hash results for within-batch duplicate detection
                    var dedupResult = deduplicationService.Deduplicate(hashResult, hashResults);

                    // Store deduplication result.
                    await _storageService.StoreDeduplicationResultAsync(dedupResult);

                    dedupResults.Add(dedupResult);

                    _logger.LogInformation(
                        $"Segment {dedupResult.SegmentId}: {dedupResult.Status}" +
                        (dedupResult.IsExactDuplicate ? $" (duplicate of {dedupResult.ExactDuplicateOf})" : "")
                    );
                }

                // Log business metrics
                var duration = DateTime.Now - startTime;
                var uniqueCount = dedupResults.Count(r => !r.IsExactDuplicate && !r.IsSimilar);
                var dupCount = dedupResults.Count - uniqueCount;
                _metricsLogger.LogPhaseEnd(
                    "DEDUPLICATION",
                    duration,
                    $"{uniqueCount} unique, {dupCount} duplicates detected"
                );

                return dedupResults;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error during deduplication: {ex.Message}");
                throw;
            }
        }
    }
}
-------------
using DocumentProcessing.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Activities
{
    /// <summary>
    /// Activity that logs final consolidated business metrics.
    /// Called at the end of orchestration to provide complete timing breakdown.
    /// </summary>
    public class LogBusinessMetricsActivity
    {
        private readonly BusinessMetricsLogger _metricsLogger;
        private readonly ILogger<LogBusinessMetricsActivity> _logger;

        public LogBusinessMetricsActivity(
            BusinessMetricsLogger metricsLogger,
            ILogger<LogBusinessMetricsActivity> logger)
        {
            _metricsLogger = metricsLogger;
            _logger = logger;
        }

        [Function(nameof(LogBusinessMetricsActivity))]
        public void Run([ActivityTrigger] BusinessMetricsRequest request)
        {
            _logger.LogInformation("🎯 Logging final business metrics for batch {BatchId}", request.BatchId);

            // Convert seconds back to TimeSpan
            var splittingDuration = TimeSpan.FromSeconds(request.SplittingSeconds);
            var dedupDuration = TimeSpan.FromSeconds(request.DeduplicationSeconds);
            var sharePointDuration = TimeSpan.FromSeconds(request.SharePointSeconds);

            var metrics = new BusinessMetrics
            {
                SplittingDuration = splittingDuration,
                DeduplicationDuration = dedupDuration,
                SharePointDuration = sharePointDuration,
                TotalDocuments = request.TotalDocuments,
                UniqueDocuments = request.UniqueDocuments,
                DuplicateDocuments = request.DuplicateDocuments,
                UploadedToSharePoint = request.UploadedToSharePoint
            };

            var totalDuration = splittingDuration + dedupDuration + sharePointDuration;

            _metricsLogger.LogProcessingComplete(request.BatchId, totalDuration, metrics);
        }
    }

    /// <summary>
    /// Request containing all timing and result data for business metrics
    /// Use double (seconds) instead of TimeSpan for Durable Functions serialization
    /// </summary>
    public class BusinessMetricsRequest
    {
        public string BatchId { get; set; } = string.Empty;
        public double SplittingSeconds { get; set; }
        public double DeduplicationSeconds { get; set; }
        public double SharePointSeconds { get; set; }
        public int TotalDocuments { get; set; }
        public int UniqueDocuments { get; set; }
        public int DuplicateDocuments { get; set; }
        public int UploadedToSharePoint { get; set; }
    }
}
-----------------
using DocumentProcessing.Models;
using DocumentProcessing.Services;
using DocumentProcessing.Orchestrations;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Activities
{
    /// <summary>
    /// Activity function that splits a PDF on black page boundaries.
    /// 
    /// This activity is called by the orchestrator to split an incoming PDF
    /// into segments. It uses the DocumentSplitterService to perform the actual splitting.
    /// </summary>
    public class SplitDocumentActivity
    {
        private readonly StorageService _storageService;
        private readonly DocumentSplitterService _splitterService;
        private readonly DocumentIntelligenceService? _docIntelligenceService;
        private readonly EnhancedBoundaryDetectorService? _enhancedBoundaryDetector;
        private readonly ILogger<SplitDocumentActivity> _logger;
        private readonly BusinessMetricsLogger _metricsLogger;
        
        /// <summary>
        /// Current batch ID - set during Run() to avoid passing through all methods.
        /// Used by ExtractSegmentsFromStorage() to construct blob paths.
        /// </summary>
        private string _currentBatchId = string.Empty;

        /// <summary>
        /// Maximum pages per chunk for large file processing.
        /// Files with more pages will be split into chunks to avoid memory issues.
        /// </summary>
        private const int MaxPagesPerChunk = 100;

        /// <summary>
        /// Page count threshold to trigger chunked processing.
        /// Files with more than this many pages will be processed in chunks.
        /// </summary>
        private const int LargeFileThreshold = 100;

        public SplitDocumentActivity(StorageService storageService,
            DocumentSplitterService splitterService,
            ILogger<SplitDocumentActivity> logger,
            BusinessMetricsLogger metricsLogger,
            DocumentIntelligenceService? docIntelligenceService = null,
            EnhancedBoundaryDetectorService? enhancedBoundaryDetector = null)
        {
            _storageService = storageService;
            _splitterService = splitterService;
            _logger = logger;
            _metricsLogger = metricsLogger;
            _docIntelligenceService = docIntelligenceService;
            _enhancedBoundaryDetector = enhancedBoundaryDetector;
        }

        [Function(nameof(SplitDocumentActivity))]
        public async Task<SplitResult> Run([ActivityTrigger] ProcessingRequest request)
        {
            var startTime = DateTime.Now;

            try
            {
                _logger.LogInformation($"Starting document split for {request.BlobName}");

                // Store batch ID for use in ExtractSegmentsFromStorage
                _currentBatchId = request.BatchId;

                // Ensure all containers exist before processing.
                await _storageService.EnsureContainersExistAsync();
                await _storageService.EnsureTableExistsAsync();

                // ⚠️ CRITICAL: Check page count BEFORE loading full PDF into memory
                // This prevents memory issues with large files (e.g., 659 pages = 90 MB)
                int pageCount = await GetPageCountFromStorageAsync(request.BlobName);
                _logger.LogInformation("{BlobName}: {PageCount} pages detected", request.BlobName, pageCount);

                // Log business metrics - processing start
                _metricsLogger.LogProcessingStart(request.BatchId, request.BlobName, pageCount);
                _metricsLogger.LogPhaseStart("DOCUMENT SPLITTING");

                // Read the incoming PDF (only after determining if we need chunking)
                byte[]? pdfBytes = null;
                if (pageCount <= LargeFileThreshold)
                {
                    // Small file: load entire PDF into memory
                    pdfBytes = await _storageService.ReadIncomingPdfAsync(request.BlobName);
                }

                List<DocumentSegment> segments;

                // Try using Document Intelligence if available
                if (_docIntelligenceService != null && _enhancedBoundaryDetector != null)
                {
                    try
                    {
                        if (pageCount > LargeFileThreshold)
                        {
                            _logger.LogInformation("⚠️ Large file detected ({PageCount} pages). Using chunked processing to avoid memory limits.", pageCount);
                            // For large files, process in chunks without loading entire file first
                            segments = await ProcessLargeFileInChunks(request.BlobName, pageCount);
                        }
                        else
                        {
                            _logger.LogInformation("Using Document Intelligence for enhanced boundary detection");

                            // pdfBytes is guaranteed to be non-null for small files
                            if (pdfBytes == null)
                                throw new InvalidOperationException("PDF bytes not loaded for small file");

                            // Call Document Intelligence for structure analysis
                            var aiAnalysis = await _docIntelligenceService.AnalyzeDocumentAsync(pdfBytes, request.BlobName);
                            
                            // Use enhanced boundary detection with AI results (includes duplicate detection)
                            var boundaries = _enhancedBoundaryDetector.DetectBoundaries(aiAnalysis, pdfBytes);
                            _logger.LogInformation("Document Intelligence found {BoundaryCount} boundaries", boundaries.Count);

                            // Split PDF using detected boundaries (pass Document Intelligence result for blank detection)
                            segments = _splitterService.SplitPdfWithBoundaries(pdfBytes, request.BlobName, boundaries, aiAnalysis);
                            _logger.LogInformation("Split with Document Intelligence: {SegmentCount} segments created", segments.Count);
                        }
                    }
                    catch (Exception aiEx)
                    {
                        _logger.LogWarning(aiEx, "Document Intelligence failed, falling back to standard splitting: {Message}", aiEx.Message);
                        
                        // Fallback: Load PDF if not already loaded
                        if (pdfBytes == null)
                            pdfBytes = await _storageService.ReadIncomingPdfAsync(request.BlobName);
                        
                        // Fall back to standard splitting
                        segments = _splitterService.SplitPdf(pdfBytes, request.BlobName);
                    }
                }
                else
                {
                    _logger.LogInformation("Document Intelligence not configured, using standard splitting");
                    
                    // Load PDF if not already loaded
                    if (pdfBytes == null)
                        pdfBytes = await _storageService.ReadIncomingPdfAsync(request.BlobName);
                    
                    // Use standard splitting
                    segments = _splitterService.SplitPdf(pdfBytes, request.BlobName);
                }

                if (segments.Count == 1)
                {
                    _logger.LogWarning($"⚠️ PDF returned as single segment (size: {segments[0].SizeBytes} bytes). Check if boundary detection worked correctly.");
                }

                _logger.LogInformation($"Split document into {segments.Count} segments");

                // ⚠️ CRITICAL FIX: Only store segments if NOT already stored by chunked processing
                // Large files (> 100 pages) are stored during ExtractSegmentsFromStorage() in ProcessLargeFileInChunks()
                // Small files need segments stored here
                bool segmentsAlreadyStored = pageCount > LargeFileThreshold;
                
                if (!segmentsAlreadyStored)
                {
                    // Store each segment to the appropriate container
                    foreach (var segment in segments)
                    {
                        var segmentNumber = segments.IndexOf(segment) + 1;
                        
                        // Determine container based on duplicate flag
                        string containerPath;
                        if (segment.DetectedAsDuplicate && segment.DuplicateOfSegmentIndex.HasValue)
                        {
                            // Minimal naming: seg{N}-p{start}-{end}-DUP.pdf
                            var duplicateFileName = $"seg{segmentNumber}-p{segment.StartPageNumber}-{segment.EndPageNumber}-DUP.pdf";
                            
                            // Simple folder structure: just segment number
                            var originalSegmentNumber = segment.DuplicateOfSegmentIndex.Value + 1;
                            var duplicateFolder = $"seg{originalSegmentNumber}";
                            containerPath = $"{request.BatchId}/exact-duplicates/{duplicateFolder}/{duplicateFileName}";
                            
                            _logger.LogInformation("Storing duplicate segment {SegNum} to: duplicates/{Path}",
                                segmentNumber, containerPath);
                        }
                        else
                        {
                            // Minimal naming: seg{N}-p{start}-{end}.pdf
                            var fileName = $"seg{segmentNumber}-p{segment.StartPageNumber}-{segment.EndPageNumber}.pdf";
                            containerPath = $"{request.BatchId}/{fileName}";
                        }
                        
                        // Use the extracted segment bytes (from SplitPdf)
                        var segmentBytes = segment.SegmentBytes ?? pdfBytes; // Fallback to original if not extracted
                        
                        if (segmentBytes == null || segmentBytes.Length == 0)
                        {
                            _logger.LogWarning("Skipping empty segment {SegmentId} (pages {Start}-{End})",
                                segment.SegmentId, segment.StartPageNumber, segment.EndPageNumber);
                            continue;
                        }
                        
                        // Store to appropriate container
                        if (segment.DetectedAsDuplicate)
                        {
                            await _storageService.StoreDuplicateSegmentAsync(containerPath, segmentBytes);
                            _logger.LogInformation("✅ Stored duplicate: {Path}",
                                containerPath);
                        }
                        else
                        {
                            await _storageService.StoreSplitSegmentAsync(containerPath, segmentBytes);
                            _logger.LogInformation("✅ Stored unique segment: {Path}",
                                containerPath);
                        }
                        
                        // CRITICAL: Store the blob PATH (not URL) so dedup activity can read it back
                        segment.OutputBlobPath = containerPath;
                        
                        // ⚠️ CRITICAL: Clear SegmentBytes BEFORE returning - byte[] cannot be JSON serialized in Durable Functions
                        segment.SegmentBytes = null;
                    }
                }
                else
                {
                    _logger.LogInformation("Segments already stored during chunked processing - skipping duplicate storage");
                }

                // Log business metrics
                var duration = DateTime.Now - startTime;
                _metricsLogger.LogPhaseEnd(
                    "DOCUMENT SPLITTING",
                    duration,
                    $"{segments.Count} documents identified from {pageCount} pages"
                );

                return new SplitResult
                {
                    BatchId = request.BatchId,
                    Segments = segments,
                    OriginalPdfBytes = null
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error splitting document: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Gets the page count from a PDF in Azure Storage without loading full content into memory.
        /// Uses streaming to minimize memory footprint for large files.
        /// </summary>
        private async Task<int> GetPageCountFromStorageAsync(string blobName)
        {
            using var stream = await _storageService.OpenIncomingPdfStreamAsync(blobName);
            using var reader = new iText.Kernel.Pdf.PdfReader(stream);
            using var pdf = new iText.Kernel.Pdf.PdfDocument(reader);
            return pdf.GetNumberOfPages();
        }

        /// <summary>
        /// Processes large PDF files in chunks to avoid memory limits.
        /// Splits file into 100-page chunks, processes each through Document Intelligence,
        /// then reassembles boundaries with adjusted page numbers.
        /// Loads the PDF from storage once and extracts chunks to minimize memory usage.
        /// </summary>
        private async Task<List<DocumentSegment>> ProcessLargeFileInChunks(string blobName, int totalPages)
        {
            var allBoundaries = new List<DocumentBoundary>();
            int chunkNumber = 1;
            int totalChunks = (int)Math.Ceiling((double)totalPages / MaxPagesPerChunk);

            _logger.LogInformation("Processing {TotalPages} pages in {ChunkCount} chunks of {ChunkSize} pages",
                totalPages, totalChunks, MaxPagesPerChunk);

            // ⚠️ MEMORY OPTIMIZATION: Do NOT load full PDF here!
            // Old code: byte[] fullPdfBytes = await _storageService.ReadIncomingPdfAsync(blobName);
            // This would load 90 MB for 659-page PDF, causing memory limit errors.
            // New approach: Extract segments directly from storage stream after boundary detection.

            // Process each chunk sequentially
            for (int startPage = 0; startPage < totalPages; startPage += MaxPagesPerChunk)
            {
                int endPage = Math.Min(startPage + MaxPagesPerChunk - 1, totalPages - 1);
                int chunkPages = endPage - startPage + 1;

                _logger.LogInformation("Processing chunk {ChunkNum}/{TotalChunks}: pages {Start}-{End} ({PageCount} pages)",
                    chunkNumber, totalChunks, startPage + 1, endPage + 1, chunkPages);

                try
                {
                    // Extract chunk directly from storage stream (avoids loading full PDF)
                    var chunkBytes = await ExtractPageRangeFromStorage(blobName, startPage, endPage);
                    _logger.LogDebug("Extracted chunk {ChunkNum}: {SizeMB:F2} MB", chunkNumber, chunkBytes.Length / 1024.0 / 1024.0);
                    
                    // Analyze chunk with Document Intelligence
                    var chunkFileName = $"{Path.GetFileNameWithoutExtension(blobName)}_chunk{chunkNumber}";
                    var aiAnalysis = await _docIntelligenceService!.AnalyzeDocumentAsync(chunkBytes, chunkFileName);
                    
                    // Detect boundaries in chunk
                    var chunkBoundaries = _enhancedBoundaryDetector!.DetectBoundaries(aiAnalysis, chunkBytes);
                    
                    // Adjust page numbers to match original PDF (0-indexed)
                    foreach (var boundary in chunkBoundaries)
                    {
                        boundary.PageNumber += startPage;
                    }
                    
                    allBoundaries.AddRange(chunkBoundaries);
                    
                    _logger.LogInformation("Chunk {ChunkNum} complete: {BoundaryCount} boundaries found",
                        chunkNumber, chunkBoundaries.Count);
                    
                    // Clear chunk bytes from memory
                    chunkBytes = null;
                    GC.Collect();
                }
                catch (Exception chunkEx)
                {
                    _logger.LogError(chunkEx, "Error processing chunk {ChunkNum}: {Message}",
                        chunkNumber, chunkEx.Message);
                    // Continue with next chunk
                }

                chunkNumber++;
            }

            _logger.LogInformation("All chunks processed. Total boundaries found: {BoundaryCount}", allBoundaries.Count);

            // ⚠️ CRITICAL FIX: For large files, extract and store segments WITHOUT loading full PDF into memory
            // This prevents memory limit errors for 600+ page PDFs
            _logger.LogInformation("Extracting segments directly from storage to avoid loading full PDF");
            
            var segments = await ExtractSegmentsFromStorage(blobName, allBoundaries, totalPages);
            _logger.LogInformation("Large file split complete: {SegmentCount} segments created and stored from chunked processing",
                segments.Count);

            // No full PDF loaded - memory optimization complete!
            GC.Collect();

            return segments;
        }

        /// <summary>
        /// Extracts a range of pages directly from Azure Storage without loading full PDF.
        /// This prevents OutOfMemoryException for large files by streaming only the needed pages.
        /// </summary>
        private async Task<byte[]> ExtractPageRangeFromStorage(string blobName, int startPage, int endPage)
        {
            // Open stream from storage (does NOT load full file into memory)
            using var storageStream = await _storageService.OpenIncomingPdfStreamAsync(blobName);
            
            // Configure PdfReader to use streaming mode (prevents internal buffering)
            var readerProperties = new iText.Kernel.Pdf.ReaderProperties();
            using var sourceReader = new iText.Kernel.Pdf.PdfReader(storageStream, readerProperties);
            using var sourcePdf = new iText.Kernel.Pdf.PdfDocument(sourceReader);

            using var outputStream = new MemoryStream();
            using var outputPdf = new iText.Kernel.Pdf.PdfDocument(new iText.Kernel.Pdf.PdfWriter(outputStream));

            // Copy only requested pages (iText7 uses 1-based indexing)
            sourcePdf.CopyPagesTo(startPage + 1, endPage + 1, outputPdf);
            outputPdf.Close();

            return outputStream.ToArray();
        }
        
        /// <summary>
        /// DEPRECATED: Old method that requires full PDF in memory.
        /// Kept for small file processing compatibility.
        /// </summary>
        private byte[] ExtractPageRange(byte[] sourcePdfBytes, int startPage, int endPage)
        {
            using var sourceStream = new MemoryStream(sourcePdfBytes);
            using var sourceReader = new iText.Kernel.Pdf.PdfReader(sourceStream);
            using var sourcePdf = new iText.Kernel.Pdf.PdfDocument(sourceReader);

            using var outputStream = new MemoryStream();
            using var outputPdf = new iText.Kernel.Pdf.PdfDocument(new iText.Kernel.Pdf.PdfWriter(outputStream));

            // Copy pages (iText7 uses 1-based indexing for CopyPagesTo)
            sourcePdf.CopyPagesTo(startPage + 1, endPage + 1, outputPdf);
            outputPdf.Close();

            return outputStream.ToArray();
        }

        /// <summary>
        /// Extracts segments from storage PDF without loading entire file into memory.
        /// Opens fresh storage stream for each segment to avoid corruption.
        /// This prevents memory issues with large PDFs (e.g., 90 MB for 659 pages).
        /// </summary>
        private async Task<List<DocumentSegment>> ExtractSegmentsFromStorage(
            string blobName, 
            List<DocumentBoundary> boundaries, 
            int totalPages)
        {
            var segments = new List<DocumentSegment>();
            
            // Calculate page ranges from boundaries (same logic as DocumentSplitterService)
            var ranges = CalculatePageRanges(boundaries, totalPages, blobName);
            
            _logger.LogInformation("Extracting {SegmentCount} segments from storage (opening fresh stream per segment)", ranges.Count);
            
            int segmentIndex = 1;
            foreach (var range in ranges)
            {
                try
                {
                    // Extract segment using fresh stream (prevents corruption from reuse)
                    var segmentBytes = await ExtractPageRangeFromStorage(blobName, range.StartPage, range.EndPage);
                    
                    if (segmentBytes == null || segmentBytes.Length == 0)
                    {
                        _logger.LogWarning("Skipping empty segment {Index} (pages {Start}-{End})",
                            segmentIndex, range.StartPage + 1, range.EndPage + 1);
                        segmentIndex++;
                        continue;
                    }
                    
                    // Determine storage path based on duplicate status
                    string blobPath;
                    if (range.IsDuplicate && range.DuplicateOfIndex.HasValue)
                    {
                        // Duplicate segment - minimal naming: seg{N}-p{start}-{end}-DUP.pdf
                        var originalSegmentNumber = range.DuplicateOfIndex.Value + 1;
                        var duplicateFileName = $"seg{segmentIndex}-p{range.StartPage + 1}-{range.EndPage + 1}-DUP.pdf";
                        var duplicateFolder = $"seg{originalSegmentNumber}";
                        blobPath = $"{_currentBatchId}/exact-duplicates/{duplicateFolder}/{duplicateFileName}";
                        
                        _logger.LogInformation("Storing duplicate segment {Idx} to duplicates container at {Path}",
                            segmentIndex, blobPath);
                        await _storageService.StoreDuplicateSegmentAsync(blobPath, segmentBytes);
                    }
                    else
                    {
                        // Unique segment - minimal naming: seg{N}-p{start}-{end}.pdf
                        var uniqueFileName = $"seg{segmentIndex}-p{range.StartPage + 1}-{range.EndPage + 1}.pdf";
                        blobPath = $"{_currentBatchId}/{uniqueFileName}";
                        _logger.LogInformation("Storing unique segment {Idx} to split container at {Path}",
                            segmentIndex, blobPath);
                        await _storageService.StoreSplitSegmentAsync(blobPath, segmentBytes);
                    }
                    
                    // Create segment metadata (required properties set first)
                    var segment = new DocumentSegment
                    {
                        SegmentId = Guid.NewGuid().ToString(),
                        SourceFileName = blobName,
                        OutputBlobPath = blobPath,  // Set after storage
                        StartPageNumber = range.StartPage + 1, // 1-indexed for display
                        EndPageNumber = range.EndPage + 1,     // 1-indexed for display
                        PageCount = range.EndPage - range.StartPage + 1,
                        SizeBytes = segmentBytes.Length,
                        DocumentType = range.DocumentType,
                        DocumentTypeConfidence = range.Confidence,
                        DetectedAsDuplicate = range.IsDuplicate,
                        DuplicateOfSegmentIndex = range.DuplicateOfIndex,
                        SegmentBytes = null  // Don't store in memory - already persisted
                    };
                    
                    segments.Add(segment);
                    
                    if (segmentIndex % 10 == 0)
                    {
                        _logger.LogInformation("Progress: Extracted {Index}/{Total} segments", segmentIndex, ranges.Count);
                    }
                    
                    segmentIndex++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error extracting segment {Index} (pages {Start}-{End}): {Message}",
                        segmentIndex, range.StartPage + 1, range.EndPage + 1, ex.Message);
                    // Continue with next segment
                    segmentIndex++;
                }
            }
            
            _logger.LogInformation("Extracted and stored {SegmentCount} segments from storage stream", segments.Count);
            return segments;
        }
        
        /// <summary>
        /// Calculates page ranges from boundaries (same logic as DocumentSplitterService.CalculateDocumentRanges).
        /// Returns list of (StartPage, EndPage, DocumentType, Confidence) tuples.
        /// </summary>
        private List<(int StartPage, int EndPage, string DocumentType, double Confidence, bool IsDuplicate, int? DuplicateOfIndex)> CalculatePageRanges(
            List<DocumentBoundary> boundaries, 
            int totalPages, 
            string fileName)
        {
            var ranges = new List<(int, int, string, double, bool, int?)>();
            
            if (boundaries.Count == 0)
            {
                // No boundaries: entire document is one segment
                ranges.Add((0, totalPages - 1, "Unknown", 1.0, false, null));
                return ranges;
            }
            
            // Sort boundaries by page number
            var sortedBoundaries = boundaries.OrderBy(b => b.PageNumber).ToList();
            
            // First segment: page 0 to first boundary
            if (sortedBoundaries[0].PageNumber > 0)
            {
                ranges.Add((0, sortedBoundaries[0].PageNumber - 1, sortedBoundaries[0].DocumentType ?? "Unknown", sortedBoundaries[0].DocumentTypeConfidence, false, null));
            }
            
            // Middle segments: between boundaries
            for (int i = 0; i < sortedBoundaries.Count - 1; i++)
            {
                int startPage = sortedBoundaries[i].PageNumber;
                int endPage = sortedBoundaries[i + 1].PageNumber - 1;
                bool isDup = sortedBoundaries[i].IsDuplicateBoundary;
                int? dupOfIdx = sortedBoundaries[i].DuplicateOfPageNumber.HasValue ? (int?)i : null;
                ranges.Add((startPage, endPage, sortedBoundaries[i].DocumentType ?? "Unknown", sortedBoundaries[i].DocumentTypeConfidence, isDup, dupOfIdx));
            }
            
            // Last segment: last boundary to end of document
            var lastBoundary = sortedBoundaries[sortedBoundaries.Count - 1];
            bool lastIsDup = lastBoundary.IsDuplicateBoundary;
            int? lastDupOfIdx = lastBoundary.DuplicateOfPageNumber.HasValue ? (int?)(sortedBoundaries.Count - 1) : null;
            ranges.Add((lastBoundary.PageNumber, totalPages - 1, lastBoundary.DocumentType ?? "Unknown", lastBoundary.DocumentTypeConfidence, lastIsDup, lastDupOfIdx));
            
            return ranges;
        }
    }
}
--------------
using DocumentProcessing.Models;
using DocumentProcessing.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Activities
{
    /// <summary>
    /// Activity function that handles storage of duplicate segments.
    /// 
    /// This activity:
    /// 1. Receives deduplication results and the original split segments.
    /// 2. For each duplicate (exact or similar), copies the segment to the duplicates container.
    /// 3. Organizes duplicates by their duplicate status for easy retrieval.
    /// 4. Returns a summary of duplicates stored.
    /// </summary>
    public class StoreDuplicatesActivity
    {
        private readonly StorageService _storageService;
        private readonly ILogger<StoreDuplicatesActivity> _logger;

        public StoreDuplicatesActivity(StorageService storageService,
            ILogger<StoreDuplicatesActivity> logger)
        {
            _storageService = storageService;
            _logger = logger;
        }

        [Function(nameof(StoreDuplicatesActivity))]
        public async Task<StoreDuplicatesSummary> Run([ActivityTrigger] StoreDuplicatesRequest request)
        {
            try
            {
                _logger.LogInformation(
                    $"Processing duplicates for batch {request.BatchId}: {request.DedupResults.Count} results"
                );

                var duplicatesStored = 0;
                var exactDuplicatesCount = 0;
                var similarDuplicatesCount = 0;

                foreach (var dedupResult in request.DedupResults)
                {
                    // Only process segments marked as duplicates
                    if (dedupResult.IsExactDuplicate || dedupResult.IsSimilar)
                    {
                        // Get the original segment PDF from split container
                        var segmentBlob = await _storageService.ReadSegmentAsync(dedupResult.BlobPath);

                        if (segmentBlob != null && segmentBlob.Length > 0)
                        {
                            // Organize duplicates by type and original reference WITH BATCH PREFIX
                            string duplicatePath;
                            if (dedupResult.IsExactDuplicate)
                            {
                                // {batchId}/exact-duplicates/{originalSegmentId}/{thisSegmentId}.pdf
                                duplicatePath = $"{request.BatchId}/exact-duplicates/{dedupResult.ExactDuplicateOf}/{dedupResult.SegmentId}.pdf";
                                exactDuplicatesCount++;
                            }
                            else // IsSimilar
                            {
                                // {batchId}/similar-duplicates/{segmentId}/similar-{minHammingDistance}.pdf
                                duplicatePath = $"{request.BatchId}/similar-duplicates/{dedupResult.SegmentId}/similar-{dedupResult.MinHammingDistance}.pdf";
                                similarDuplicatesCount++;
                            }

                            // Store in duplicates container
                            await _storageService.StoreDuplicateSegmentAsync(duplicatePath, segmentBlob);

                            duplicatesStored++;

                            _logger.LogInformation(
                                $"Stored duplicate: {dedupResult.SegmentId} -> {duplicatePath}"
                            );
                        }
                    }
                }

                var summary = new StoreDuplicatesSummary
                {
                    BatchId = request.BatchId,
                    TotalDuplicatesProcessed = request.DedupResults.Count(r => r.IsExactDuplicate || r.IsSimilar),
                    ExactDuplicatesStored = exactDuplicatesCount,
                    SimilarDuplicatesStored = similarDuplicatesCount,
                    ProcessedAt = DateTime.UtcNow
                };

                _logger.LogInformation(
                    $"Batch {request.BatchId}: Stored {duplicatesStored} duplicates " +
                    $"({exactDuplicatesCount} exact, {similarDuplicatesCount} similar)"
                );

                return summary;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error storing duplicates: {ex.Message}");
                throw;
            }
        }
    }

    /// <summary>
    /// Request object for StoreDuplicatesActivity.
    /// </summary>
    public class StoreDuplicatesRequest
    {
        public required string BatchId { get; set; }
        public required List<DeduplicationResult> DedupResults { get; set; }
    }

    /// <summary>
    /// Summary of duplicate storage operation.
    /// </summary>
    public class StoreDuplicatesSummary
    {
        public required string BatchId { get; set; }
        public int TotalDuplicatesProcessed { get; set; }
        public int ExactDuplicatesStored { get; set; }
        public int SimilarDuplicatesStored { get; set; }
        public DateTime ProcessedAt { get; set; }
    }
}
--------------
using DocumentProcessing.Models;
using DocumentProcessing.Services;
using DocumentProcessing.Orchestrations;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace DocumentProcessing.Activities
{
    /// <summary>
    /// Activity function that stores processing results and generates summaries.
    /// 
    /// This activity:
    /// 1. Compiles statistics from deduplication results.
    /// 2. Generates a JSON summary of the processing batch.
    /// 3. Writes the summary to the results container.
    /// 4. Returns a ProcessingSummary to the orchestrator.
    /// </summary>
    public class StoreResultsActivity
    {
        private readonly StorageService _storageService;
        private readonly ILogger<StoreResultsActivity> _logger;

        public StoreResultsActivity(StorageService storageService,
            ILogger<StoreResultsActivity> logger)
        {
            _storageService = storageService;
            _logger = logger;
        }

        [Function(nameof(StoreResultsActivity))]
        public async Task<ProcessingSummary> Run([ActivityTrigger] StoreResultsRequest request)
        {
            try
            {
                _logger.LogInformation(
                    $"Storing results for batch {request.BatchId}"
                );

                // Compile statistics.
                var totalSegments = request.DedupResults.Count;
                var uniqueSegments = request.DedupResults.Count(r => r.Status == "Unique");
                var exactDuplicates = request.DedupResults.Count(r => r.Status == "ExactDuplicate");
                var similarSegments = request.DedupResults.Count(r => r.Status == "Similar");

                // Create summary object.
                var summary = new ProcessingSummary
                {
                    BatchId = request.BatchId,
                    TotalSegments = totalSegments,
                    UniqueSegments = uniqueSegments,
                    ExactDuplicates = exactDuplicates,
                    SimilarSegments = similarSegments,
                    ProcessedAt = DateTime.UtcNow,
                    Status = "Completed",
                    Message = $"Processed {totalSegments} segments: " +
                             $"{uniqueSegments} unique, " +
                             $"{exactDuplicates} exact duplicates, " +
                             $"{similarSegments} similar"
                };

                // Create a detailed JSON summary.
                var detailedSummary = new
                {
                    summary.BatchId,
                    summary.TotalSegments,
                    summary.UniqueSegments,
                    summary.ExactDuplicates,
                    summary.SimilarSegments,
                    summary.ProcessedAt,
                    summary.Status,
                    summary.Message,
                    DeduplicationDetails = request.DedupResults.Select(r => new
                    {
                        r.SegmentId,
                        r.BlobPath,
                        r.Status,
                        r.ExactDuplicateOf,
                        r.SimilarSegmentIds,
                        r.MinHammingDistance
                    }).ToList()
                };

                // Write detailed summary to results container.
                var jsonSummary = JsonSerializer.Serialize(detailedSummary, 
                    new JsonSerializerOptions { WriteIndented = true });
                await _storageService.WriteResultsSummaryAsync(request.BatchId, jsonSummary);

                _logger.LogInformation(
                    $"Batch {request.BatchId} completed: {summary.Message}"
                );

                return summary;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error storing results: {ex.Message}");
                throw;
            }
        }
    }
}
----------------
using DocumentProcessing.Models;
using DocumentProcessing.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace DocumentProcessing.Activities
{
    /// <summary>
    /// Activity that syncs processed documents to SharePoint for governance and user access.
    /// This is a non-critical, best-effort operation that runs after all blob operations complete.
    /// </summary>
    public class SyncToSharePointActivity
    {
        private readonly StorageService _storageService;
        private readonly SharePointSyncService _sharePointService;
        private readonly ILogger<SyncToSharePointActivity> _logger;
        private readonly BusinessMetricsLogger _metricsLogger;

        public SyncToSharePointActivity(
            StorageService storageService,
            SharePointSyncService sharePointService,
            ILogger<SyncToSharePointActivity> logger,
            BusinessMetricsLogger metricsLogger)
        {
            _storageService = storageService;
            _sharePointService = sharePointService;
            _logger = logger;
            _metricsLogger = metricsLogger;
        }

        [Function(nameof(SyncToSharePointActivity))]
        public async Task<SharePointSyncSummary> Run(
            [ActivityTrigger] SharePointSyncRequest request)
        {
            var startTime = DateTime.Now;

            _logger.LogInformation("📋 ===== SharePoint Sync Activity Started =====");
            _logger.LogInformation("Batch: {BatchId}, Total Segments: {TotalSegments}, Skip Duplicates: {SkipDuplicates}",
                request.BatchId, request.Segments?.Count ?? 0, request.SkipDuplicates);

            _metricsLogger.LogPhaseStart("SHAREPOINT UPLOAD");

            if (!_sharePointService.IsEnabled)
            {
                _logger.LogWarning("⚠️ SharePoint sync DISABLED - skipping all uploads");
                _metricsLogger.LogPhaseEnd("SHAREPOINT UPLOAD", DateTime.Now - startTime, "DISABLED");
                return new SharePointSyncSummary
                {
                    BatchId = request.BatchId,
                    TotalSegments = request.Segments?.Count ?? 0,
                    UploadedCount = 0,
                    SkippedCount = request.Segments?.Count ?? 0,
                    FailedCount = 0,
                    IsEnabled = false
                };
            }
            
            _logger.LogInformation("✅ SharePoint service is ENABLED - proceeding with sync");

            var summary = new SharePointSyncSummary
            {
                BatchId = request.BatchId,
                TotalSegments = request.Segments?.Count ?? 0,
                IsEnabled = true
            };

            // Sync each segment to SharePoint (best-effort)
            // Unique segments go to: batch-{batchId}/
            // Duplicates go to: batch-{batchId}/duplicates/
            var segmentNumber = 0;
            foreach (var segment in request.Segments ?? Enumerable.Empty<DocumentSegment>())
            {
                segmentNumber++;
                _logger.LogInformation("📄 Processing segment {Current}/{Total}: {SegmentId}", 
                    segmentNumber, request.Segments?.Count ?? 0, segment.SegmentId);
                
                try
                {
                    // ✅ FIX #2: Add null check for OutputBlobPath
                    if (string.IsNullOrEmpty(segment.OutputBlobPath))
                    {
                        _logger.LogError("⚠️ Segment {SegmentId} has null or empty OutputBlobPath - skipping", segment.SegmentId);
                        summary.SkippedCount++;
                        continue;
                    }

                    // Read from correct container based on duplicate status
                    byte[]? segmentBytes;
                    string blobPath;

                    if (segment.IsExactDuplicate || segment.IsSimilar || segment.DetectedAsDuplicate)
                    {
                        // ✅ FIX: For duplicates, OutputBlobPath already contains the correct path
                        // SplitDocumentActivity stores duplicates with the full path already set
                        // Format: {batchId}/exact-duplicates/seg{N}/seg{M}-p{start}-{end}-DUP.pdf

                        blobPath = segment.OutputBlobPath;
                        _logger.LogDebug("Reading duplicate from duplicates container: {BlobPath}", blobPath);
                        segmentBytes = await _storageService.ReadDuplicateSegmentAsync(blobPath);
                    }
                    else
                    {
                        // Unique segments are in split-documents container with original path
                        blobPath = segment.OutputBlobPath;
                        _logger.LogDebug("Reading unique segment from split container: {BlobPath}", blobPath);
                        segmentBytes = await _storageService.ReadSegmentAsync(blobPath);
                    }
                    
                    if (segmentBytes == null || segmentBytes.Length == 0)
                    {
                        _logger.LogError("❌ Segment not found or empty: {Path}", blobPath);
                        summary.FailedCount++;
                        continue;
                    }

                    _logger.LogDebug("Segment read successfully: {Size} bytes", segmentBytes.Length);

                    // ✅ FIX: Determine folder path based on duplicate status
                    // Check ALL duplicate flags: IsExactDuplicate, IsSimilar, DetectedAsDuplicate
                    string folderPath;
                    string segmentType;

                    bool isDuplicate = segment.IsExactDuplicate || segment.IsSimilar || segment.DetectedAsDuplicate;

                    if (isDuplicate)
                    {
                        // Upload duplicates to: batch-{batchId}/duplicates/
                        folderPath = $"batch-{request.BatchId}/duplicates";
                        if (segment.IsExactDuplicate)
                            segmentType = "EXACT-DUPLICATE";
                        else if (segment.IsSimilar)
                            segmentType = "SIMILAR";
                        else
                            segmentType = "WITHIN-BATCH-DUPLICATE";

                        _logger.LogInformation("🔁 Duplicate detected: {SegmentId} - Type: {Type}", segment.SegmentId, segmentType);
                    }
                    else
                    {
                        // Upload unique segments to: batch-{batchId}/
                        folderPath = $"batch-{request.BatchId}";
                        segmentType = "UNIQUE";
                    }

                    // ✅ FIX: Extract filename from OutputBlobPath to preserve original naming
                    var fileName = Path.GetFileName(segment.OutputBlobPath);
                    _logger.LogInformation("Starting upload of {Type} to folder: {Folder}, file: {FileName}",
                        segmentType, folderPath, fileName);
                    
                    var success = await _sharePointService.UploadDocumentAsync(
                        folderPath, 
                        fileName, 
                        segmentBytes);

                    if (success)
                    {
                        summary.UploadedCount++;
                        _logger.LogInformation("Upload completed - {Type}: {FileName}", segmentType, fileName);
                    }
                    else
                    {
                        summary.FailedCount++;
                        _logger.LogError("❌ FAILED: Could not upload {FileName} to SharePoint", fileName);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "❌ EXCEPTION while syncing segment {SegmentId}: {ErrorMessage}", segment.SegmentId, ex.Message);
                    summary.FailedCount++;
                }
            }

            _logger.LogInformation("📊 ===== SharePoint Sync Activity Complete =====");
            _logger.LogInformation(
                "Batch {BatchId} Results: ✅ {Uploaded} uploaded, ⏭️  {Skipped} skipped, ❌ {Failed} failed (Total: {Total})",
                request.BatchId, summary.UploadedCount, summary.SkippedCount, summary.FailedCount, summary.TotalSegments);

            // Log business metrics
            var duration = DateTime.Now - startTime;
            _metricsLogger.LogPhaseEnd(
                "SHAREPOINT UPLOAD",
                duration,
                $"{summary.UploadedCount} files uploaded successfully"
            );

            return summary;
        }
    }

    public class SharePointSyncRequest
    {
        public string BatchId { get; set; } = string.Empty;
        public List<DocumentSegment> Segments { get; set; } = new();
        public bool SkipDuplicates { get; set; } = true;
    }

    public class SharePointSyncSummary
    {
        public string BatchId { get; set; } = string.Empty;
        public int TotalSegments { get; set; }
        public int UploadedCount { get; set; }
        public int SkippedCount { get; set; }
        public int FailedCount { get; set; }
        public bool IsEnabled { get; set; }
    }
}
