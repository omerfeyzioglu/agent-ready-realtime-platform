package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// sourceKey is where the frozen source file of a month lives in the bucket.
func sourceKey(month string) string {
	y, m, _ := strings.Cut(month, "-")
	return fmt.Sprintf("source/tlc/fhvhv/year=%s/month=%s/fhvhv_tripdata_%s.parquet", y, m, month)
}

// download copies each month's source file from S3 into dir and returns the
// local paths in month order. Parquet needs random access (the footer is at
// the end), so the file is read from local disk rather than streamed. A file
// already present with the right size is reused. Credentials come from the
// default chain: in the cluster that is EKS Pod Identity.
func download(ctx context.Context, bucket, region, dir string, months []string) ([]string, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	var paths []string
	for _, month := range months {
		key := sourceKey(month)
		path := filepath.Join(dir, filepath.Base(key))

		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			return nil, fmt.Errorf("s3://%s/%s: %w", bucket, key, err)
		}
		if st, err := os.Stat(path); err == nil && head.ContentLength != nil && st.Size() == *head.ContentLength {
			logf("source %s already downloaded", month)
			paths = append(paths, path)
			continue
		}

		logf("downloading s3://%s/%s", bucket, key)
		obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			return nil, err
		}
		tmp := path + ".part"
		f, err := os.Create(tmp)
		if err != nil {
			obj.Body.Close()
			return nil, err
		}
		n, err := io.Copy(f, obj.Body)
		obj.Body.Close()
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, err
		}
		if err := os.Rename(tmp, path); err != nil {
			return nil, err
		}
		logf("downloaded %s (%d MB)", month, n>>20)
		paths = append(paths, path)
	}
	return paths, nil
}
