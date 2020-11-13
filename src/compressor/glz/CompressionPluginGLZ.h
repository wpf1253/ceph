/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 XSKY Inc.
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_COMPRESSION_PLUGIN_GLZ_H
#define CEPH_COMPRESSION_PLUGIN_GLZ_H

// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "compressor/CompressionPlugin.h"
#include "GLZCompressor.h"
// -----------------------------------------------------------------------------

class CompressionPluginGLZ : public CompressionPlugin {

public:

  explicit CompressionPluginGLZ(CephContext* cct) : CompressionPlugin(cct)
  {}

  int factory(CompressorRef *cs, std::ostream *ss) override {
    if (compressor == 0) {
      GLZCompressor *interface = new GLZCompressor(cct);
      compressor = CompressorRef(interface);
    }
    *cs = compressor;
    return 0;
  }
};

#endif
