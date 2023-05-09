# froze_string_literal: true

require "csv"

module JobIteration
    class ActiveStorageCsvEnumerator
        
        """
        @param [ActiveStorage::Attachment, ActiveStorage::Blob] attachment_or_blob
        @param [Integer] cursor - byte offset to start reading from
        @param [Boolean] include_header - does the file have a header?, defaults to true.
        @param [Integer] batch_size - # of rows to read
        @param [Integer] chunk_size - # of bytes to read at a time
        @param [Hash] parse_opts - options to pass to CSV
        """
        def initialize(attachment_or_blob, cursor: nil, include_header: true, batch_size: 100, chunk_size: 256, **parse_opts)
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
            @quote_char = @parse_opts[:quote_char] || "\""
            
            # read header
            if include_header
                hdr, hdr_cursor = ingest_row(0)
                @header = CSV.parse(hdr)[0]
                @cursor = hdr_cursor if hdr_cursor > @cursor
                @parse_opts[:headers] = @header if @parse_opts[:headers].present? && @parse_opts[:headers] != false
            end
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

        attr_accessor :header

        private

        def ingest_batch(cursor)
            rows = ""
            
            while rows.count(@row_sep) < @batch_size && rows.count(@quote_char) % 2 == 0
                chunk = download_chunk(cursor)
                break if chunk.nil? || chunk.empty?

                rows += chunk
                cursor += chunk.size
            end

            # trim any excess bytes read
            row_sep_index = find_nth_index(rows, @row_sep, @batch_size)
            cursor = cursor - (rows.size - row_sep_index - @row_sep.size)
            rows = rows[0..row_sep_index + @row_sep.size - 1]
        
            [rows, cursor]
        end

        def ingest_row(cursor)
            row = ""

            until row.include?(@row_sep) && row.count(@quote_char) % 2 == 0
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
            return nil if cursor >= @blob.byte_size
            
            @blob.download_chunk(cursor..[cursor + @chunk_size - 1, @blob.byte_size].min)
        end

        def find_nth_index(str, substr, n)
            return nil if n < 1
            index = -1
            
            while n > 0
                i = str.index(substr, index + 1)
                return nil || index if i.nil?

                index = i
                n -= 1
            end
        index
        end
    end
end