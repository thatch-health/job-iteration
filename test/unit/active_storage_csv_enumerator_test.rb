
require "test_helper"

module JobIteration
    class ActiveStorageCsvEnumeratorTest < ActiveSupport::TestCase
        test "#initialize include_header respositions cursor" do
            csv = read_csv
            csv.shift

            enum = JobIteration::ActiveStorageCsvEnumerator.new(mock_blob, include_header: true)

            assert_equal(csv.headers, enum.header)
            assert_equal(25, enum.instance_variable_get(:@cursor))
        end

        test "#initialize headers parse opt is set" do
            enum = JobIteration::ActiveStorageCsvEnumerator.new(mock_blob, include_header: true, headers: true)

            assert_equal(enum.header, enum.instance_variable_get(:@parse_opts)[:headers])
            
        end

        test "#initialize include_header false does not reposition cursor" do
            enum = JobIteration::ActiveStorageCsvEnumerator.new(mock_blob, include_header: false)

            assert_equal(0, enum.instance_variable_get(:@cursor)) 
        end

        test "#rows returns same values as CSV" do
            csv = read_csv

            enum = JobIteration::ActiveStorageCsvEnumerator.new(mock_blob, include_header: true, headers: true)

            enum.rows.each do |row, cursor|
                assert_equal(csv.shift, row.shift)
            end
        end

        test "#batches returns same values as CSV" do
            csv = read_csv

            enum = JobIteration::ActiveStorageCsvEnumerator.new(mock_blob, include_header: true, headers: true, batch_size: 2)

            enum.batches.each do |batch, cursor|
                batch.each do |row|
                    assert_equal(csv.shift, row)
                end
            end
        end

        test "#batches returns same values as CSV when batch_size is larger than file" do
            csv = read_csv

            enum = JobIteration::ActiveStorageCsvEnumerator.new(mock_blob, include_header: true, headers: true, batch_size: 100)

            enum.batches.each do |batch, cursor|
                batch.each do |row|
                    assert_equal(csv.shift, row)
                end
            end
        end

        test "#rows returns same values as CSV for file with quotes" do
            csv = CSV.new(File.open("test/support/sample_csv_with_headers_and_quotes.csv"), headers: true)

            blob = MockActiveStorageBlob.new("test/support/sample_csv_with_headers_and_quotes.csv")
            enum = JobIteration::ActiveStorageCsvEnumerator.new(blob, include_header: true, headers: true)

            enum.rows.each do |row, cursor|
                assert_equal(csv.shift, row.shift)
            end
        end

        test "#batches returns same values as CSV for file with quotes" do
        csv = CSV.new(File.open("test/support/sample_csv_with_headers_and_quotes.csv"), headers: true)

        blob = MockActiveStorageBlob.new("test/support/sample_csv_with_headers_and_quotes.csv")
        enum = JobIteration::ActiveStorageCsvEnumerator.new(blob, include_header: true, headers: true)

        enum.batches.each do |batch, cursor|
            batch.each do |row|
                assert_equal(csv.shift, row)
            end
        end 
        end

        private

        def read_csv
            CSV.new(File.open("test/support/sample_csv_with_headers.csv"), headers: true)
        end

        def mock_blob
            MockActiveStorageBlob.new("test/support/sample_csv_with_headers.csv")
        end
    end
end