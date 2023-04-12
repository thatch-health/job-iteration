# frozen_string_literal: true

"""
things left to address:
* Quotes in BlobIO are causing the cursor to jump
* Passing a StringIO causes the pos to return the last value.
"""

module JobIteration
    class CsvIoEnumerator
        class BlobIO
            def initialize(blob, offset=0)
                @blob = if blob.respond_to?(:blob)
                    blob.blob
                else
                    blob
                end

                raise(ArgumentError, "Content Type must be \"text/csv\", got #{@blob.content_type}") unless @blob.content_type == "text/csv"

                @cursor = offset
              end
            
              def gets(row_sep, limit)
                return nil if @cursor >= @blob.byte_size
            
                row = ""
                until row.include?(row_sep)
                  chunk = read_chunk(256)
                  break if chunk.nil? || chunk.empty?
            
                  if chunk.include?(row_sep)
                    row += chunk[0..chunk.index(row_sep) + row_sep.size - 1]
                    @cursor += chunk.index(row_sep) + row_sep.size
                  else
                    row += chunk
                    @cursor += chunk.size
                  end
                end
            
                row
              end
            
              def pos
                @cursor
              end
            
              private
            
              def read_chunk(limit)
                @blob.download_chunk(@cursor..@cursor + limit  - 1)
              end
        end

        def initialize(io_or_blob, cursor: nil, **parse_opts)
            @io = if io_or_blob.respond_to?(:gets)  
                io_or_blob.seek(cursor || 0, IO::SEEK_SET)
                io_or_blob
            else
                BlobIO.new(io_or_blob, cursor || 0)
            end

            @cursor = cursor
            @csv = CSV.new(@io, **parse_opts)
        end

        def rows
            @csv.lazy.collect do |row| 
              @cursor += row.bytesize
              [row, @io.pos, @cursor]
            end
            .to_enum
        end

        def batches(batch_size: 100)
            @csv.lazy
                .each_slice(batch_size)
                .collect { |rows| [rows, @io.pos] }
                .to_enum
        end

        attr_accessor :io, :csv

    end
end