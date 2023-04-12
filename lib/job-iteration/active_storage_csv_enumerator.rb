# froze_string_literal: true

module JobIteration
    class ActiveStorageCsvEnumerator
        
        """
        @param [ActiveStorage::Attachment, ActiveStorage::Blob] attachment_or_blob
        @param [Integer] cursor - byte offset to start reading from
        @param [Boolean] include_header
        @param [Integer] batch_size - # of rows to read
        @param [Integer] chunk_size - # of bytes to read at a time
        @param [Hash] parse_opts - options to pass to CSV
        """
        def initialize(attachment_or_blob, cursor: nil, include_header: false, batch_size: 100, chunk_size: 256, **parse_opts)
            @blob = if attachment_or_blob.respond_to?(:blob)
                attachment_or_blob.blob
            else
                attachment_or_blob
            end

            @cursor = cursor || 0
            @batch_size = batch_size
            @chunk_size = chunk_size
            @parse_opts = parse_opts

            @row_sep = @parse_opts[:row_sep] || "\n"
            
            # read header
            hdr, hdr_cursor = ingest_row(0)
            @cursor = hdr_cursor if hdr_cursor > @cursor
            @parse_opts[:headers] = CSV.parse(hdr)[0] if include_header
        end

        def rows
            Enumerator.new() do |yielder|
                while (row, cursor = ingest_row(@cursor))
                    break if row.empty?
                    @cursor= cursor
                    yielder.yield(CSV.new(row, **@parse_opts), @cursor)
                end
            end
        end

        def batches
            Enumerator.new do |yielder|
                while (rows, cursor = ingest_batch(@cursor))
                    break if rows.empty?
                    @cursor = cursor
                    yielder.yield(CSV.new(rows, **@parse_opts), @cursor)
                end
            end
        end

        private

        def ingest_batch(cursor)
            rows = ""
            
            while rows.count(@row_sep) < @batch_size
                chunk = download_chunk(cursor)
                break if chunk.nil? || chunk.empty?

                rows += chunk
                cursor += chunk.size
            end

            # trim any excess bytes read
            cursor = cursor - (rows.size - rows.rindex(@row_sep) + @row_sep.size)
            rows = rows[0..rows.rindex(@row_sep) + @row_sep.size - 1]
        
            [rows, cursor]
        end

        def ingest_row(cursor)
            row = ""
            until row.include?(@row_sep)
                chunk = download_chunk(cursor)
                break if chunk.nil? || chunk.empty?

                if chunk.include?(@row_sep)
                    row += chunk[0..chunk.index(@row_sep) + @row_sep.size - 1]
                    cursor+= chunk.index(@row_sep) + @row_sep.size
                else
                    row += chunk
                    cursor += chunk.size
                end
            end

            [row, cursor]
        end

        def download_chunk(cursor)
            @blob.download_chunk(cursor..cursor + @chunk_size - 1)
        end
    end
end