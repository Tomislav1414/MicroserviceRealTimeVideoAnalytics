package main

import (
	"context"
	"io"
	"log"

	"github.com/docker/docker/api/types/image"
)

func (s *Server) ensureImage(ctx context.Context, img string) error {
	images, err := s.docker.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return err
	}
	for _, i := range images {
		for _, tag := range i.RepoTags {
			if tag == img {
				return nil // already present
			}
		}
	}
	log.Printf("pulling image %s...", img)
	rc, err := s.docker.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		return err
	}
	defer rc.Close()
	io.Copy(io.Discard, rc) // drain so pull completes
	return nil
}
