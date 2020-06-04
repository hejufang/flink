#!/usr/bin/env python
# -*- coding:utf-8 -*-

import glob
import os
import re
import shutil
import zipfile


class JarUtil(object):
    RESOURCES_NAME = "resources"

    @staticmethod
    def open_jar(base_jar):
        """Open the base jar file."""
        if not os.path.exists(base_jar):
            raise Exception("Base jar not found " + str(base_jar))

        if not zipfile.is_zipfile(base_jar):
            raise Exception("Base jar is not a jar file: " + str(base_jar))

        zip_file = zipfile.ZipFile(base_jar, "r")
        return zip_file

    @staticmethod
    def zip_dir(src, arc):
        """Build a zip archive from the specified src.

        Note: If the archive already exists, files will be simply
        added to it, but the original archive will not be replaced.
        """
        src_re = re.compile(src + "/*")
        for root, dirs, files in os.walk(src):
            # hack for copying everithing but the top directory
            prefix = re.sub(src_re, "", root)
            for f in files:
                # zipfile creates directories if missing
                arc.write(os.path.join(root, f), os.path.join(prefix, f),
                          zipfile.ZIP_DEFLATED)

    @staticmethod
    def pack_jar(tmp_dir, output_jar):
        """Build a jar from the temporary directory."""
        zf = zipfile.ZipFile(output_jar, "w")
        try:
            JarUtil.zip_dir(tmp_dir, zf)
        finally:
            zf.close()

    @staticmethod
    def content_to_copy(src, exclude):
        """Return a set of top-level content to copy, excluding exact matches
        from the exclude list.
        """
        content = set(glob.glob(os.path.join(src, "*")))
        content -= set(exclude)
        return content

    @staticmethod
    def copy_dir_content(src, dst, exclude):
        """Copy the content of a directory excluding the yaml file
        and requirements file.

        This functions is used instead of shutil.copytree() because
        the latter always creates a top level directory, while only
        the content need to be copied in this case.
        """
        content = JarUtil.content_to_copy(src, exclude)
        for t in content:
            if os.path.isdir(t):
                shutil.copytree(t, os.path.join(dst, os.path.basename(t)),
                                symlinks=True)
            else:
                shutil.copy2(t, dst)

    @staticmethod
    def expand_path(path):
        """Return the corresponding absolute path after variables expansion."""
        return os.path.abspath(os.path.expanduser(path))

    @staticmethod
    def create_flink_jar(yaml_file, resource_dir,
                         output_jar, zip_file, tmp_dir):
        """Coordinate the creation of the the topology JAR:

            - Validate the topology
            - Extract the base JAR into a temporary directory
            - Copy all source files into the directory
            - Re-pack the temporary directory into the final JAR
        """
        # Extract pyleus base jar content in a tmp dir
        zip_file.extractall(tmp_dir)
        # Create resources directory
        tmp_resource_dir = os.path.join(tmp_dir, JarUtil.RESOURCES_NAME)

        # add yaml file
        shutil.copy2(yaml_file, tmp_dir)

        os.mkdir(tmp_resource_dir)
        # Add the resources dir
        JarUtil.copy_dir_content(
            src=resource_dir,
            dst=tmp_resource_dir,
            exclude=[output_jar]
        )

        # Pack the tmp directory into a jar
        JarUtil.pack_jar(tmp_dir, output_jar)
