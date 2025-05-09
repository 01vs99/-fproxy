/*
 * Copyright 2023 Hongen Wang All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:proxypin/network/channel/channel_context.dart';
import 'package:proxypin/network/channel/host_port.dart';
import 'package:proxypin/network/http/h2/h2_codec.dart';
import 'package:proxypin/network/http/http.dart';
import 'package:proxypin/network/http/http_headers.dart';
import 'package:proxypin/network/channel/network.dart';
import 'package:proxypin/network/util/byte_buf.dart';
import 'package:proxypin/network/util/logger.dart';
import 'package:proxypin/network/util/system_proxy.dart';
import 'package:proxy_manager/proxy_manager.dart';

import '../channel/channel.dart';
import 'codec.dart';
import 'h2/frame.dart';

class HttpClients {
  static Future<Channel> startConnect(HostAndPort hostAndPort, {Duration timeout = const Duration(seconds: 3)}) {
    String host = hostAndPort.host;
    //说明支持ipv6
    if (host.startsWith("[") && host.endsWith(']')) {
      host = host.substring(1, host.length - 1);
    }

    return Socket.connect(host, hostAndPort.port, timeout: timeout).then((socket) {
      if (socket.address.type != InternetAddressType.unix) {
        socket.setOption(SocketOption.tcpNoDelay, true);
      }
      return Channel(socket);
    });
  }

  ///代理建立连接
  static Future<Channel> proxyConnect(
      HttpRequest request, HostAndPort hostAndPort, ChannelHandler<HttpResponse> handler, ChannelContext channelContext,
      {ProxyInfo? proxyInfo}) async {
    var client = Client()..initChannel((channel) => channel.dispatcher.channelHandle(HttpClientCodec(), handler));

    if (proxyInfo == null) {
      var proxyTypes = hostAndPort.isSsl() ? ProxyTypes.https : ProxyTypes.http;
      proxyInfo = await SystemProxy.getSystemProxy(proxyTypes);
    }

    HostAndPort connectHost = proxyInfo == null ? hostAndPort : HostAndPort.host(proxyInfo.host, proxyInfo.port!);
    var channel = await client.connect(connectHost, channelContext);

    if (proxyInfo != null) {
      await connectRequest(hostAndPort, channel, proxyInfo: proxyInfo);
    }

    if (hostAndPort.isSsl()) {
      await channel.startSecureSocket(channelContext,
          host: hostAndPort.host, supportedProtocols: request.protocolVersion == "HTTP/2" ? ["h2"] : null);

      if (channelContext.serverChannel?.selectedProtocol == "h2") {
        await Http2ClientHandler(handler).listen(channel, channelContext);
      } else {
        channel.dispatcher.listen(channel, channelContext);
      }
    }

    logger.d(
        "request ${hostAndPort.host}:${hostAndPort.port} ${request.protocolVersion} ${channelContext.serverChannel?.selectedProtocol}");

    return channel;
  }

  ///发起代理连接请求
  static Future<Channel> connectRequest(HostAndPort hostAndPort, Channel channel, {ProxyInfo? proxyInfo}) async {
    ChannelHandler handler = channel.dispatcher.handler;
    //代理 发送connect请求
    var httpResponseHandler = HttpResponseHandler();
    channel.dispatcher.handler = httpResponseHandler;

    HttpRequest proxyRequest = HttpRequest(HttpMethod.connect, '${hostAndPort.host}:${hostAndPort.port}');
    proxyRequest.headers.set(HttpHeaders.HOST, '${hostAndPort.host}:${hostAndPort.port}');

    //proxy Authorization
    if (proxyInfo?.isAuthenticated == true) {
      String auth = base64Encode(utf8.encode("${proxyInfo?.username}:${proxyInfo?.password}"));
      proxyRequest.headers.set(HttpHeaders.PROXY_AUTHORIZATION, 'Basic $auth');
    }

    await channel.write(proxyRequest);
    var response = await httpResponseHandler.getResponse(const Duration(seconds: 5));

    channel.dispatcher.handler = handler;

    if (!response.status.isSuccessful()) {
      throw Exception("$hostAndPort Proxy failed to establish tunnel "
          "(${response.status.code} ${response..status.reasonPhrase})");
    }

    return channel;
  }

  /// 建立连接
  static Future<Channel> connect(Uri uri, ChannelHandler handler, ChannelContext channelContext) async {
    Client client = Client()
      ..initChannel((channel) => channel.dispatcher.handle(HttpResponseCodec(), HttpRequestCodec(), handler));
    if (uri.scheme == "https" || uri.scheme == "wss") {
      return client.secureConnect(HostAndPort.of(uri.toString()), channelContext);
    }

    return client.connect(HostAndPort.of(uri.toString()), channelContext);
  }

  /// 发送get请求
  static Future<HttpResponse> get(String url, {Duration timeout = const Duration(seconds: 3)}) async {
    HttpRequest msg = HttpRequest(HttpMethod.get, url);
    return request(HostAndPort.of(url), msg, timeout: timeout);
  }

  /// 发送请求
  static Future<HttpResponse> request(HostAndPort hostAndPort, HttpRequest request,
      {Duration timeout = const Duration(seconds: 3)}) async {
    var httpResponseHandler = HttpResponseHandler();

    var client = Client()
      ..initChannel(
          (channel) => channel.dispatcher.handle(HttpResponseCodec(), HttpRequestCodec(), httpResponseHandler));

    ChannelContext channelContext = ChannelContext();
    Channel channel = await client.connect(hostAndPort, channelContext);
    await channel.write(request);

    return httpResponseHandler.getResponse(timeout).whenComplete(() => channel.close());
  }

  /// 发送代理请求
  static Future<HttpResponse> proxyRequest(HttpRequest request,
      {ProxyInfo? proxyInfo, Duration timeout = const Duration(seconds: 30)}) async {
    if (request.headers.host == null || request.headers.host?.trim().isEmpty == true) {
      try {
        var uri = Uri.parse(request.requestUrl);
        request.headers.host = '${uri.host}${uri.hasPort ? ':${uri.port}' : ''}';
      } catch (_) {}
    }

    ChannelContext channelContext = ChannelContext();
    var httpResponseHandler = HttpResponseHandler();
    request.hostAndPort ??= HostAndPort.of(request.requestUrl);

    Channel channel =
        await proxyConnect(request, proxyInfo: proxyInfo, request.hostAndPort!, httpResponseHandler, channelContext);

    if (!request.uri.startsWith("/")) {
      Uri? uri = request.requestUri;
      request = request.copy(uri: '${uri!.path}${uri.hasQuery ? '?${uri.query}' : ''}');
    }

    if (channel.selectedProtocol == 'h2') {
      request.headers.remove(HttpHeaders.HOST);
      request.streamId = 1;
    }
    await channel.write(request);
    return httpResponseHandler.getResponse(timeout).whenComplete(() => channel.close());
  }
}

class Http2ClientHandler {
  static const int FLAG_ACK = 0x1;

  ByteBuf byteBuf = ByteBuf();
  Http2ResponseDecoder decoder = Http2ResponseDecoder();
  final ChannelHandler<HttpResponse> handler;

  Http2ClientHandler(this.handler);

  Future<void> listen(Channel channel, ChannelContext channelContext) async {
    channel.dispatcher.encoder = Http2RequestDecoder();
    channel.dispatcher.decoder = decoder;

    channel.socket.listen((data) => onData(channelContext, channel, data),
        onError: (error, trace) => handler.exceptionCaught(channelContext, channel, error, trace: trace),
        onDone: () => handler.channelInactive(channelContext, channel));

    channel.writeBytes(Http2Codec.connectionPrefacePRI);
  }

  onData(ChannelContext channelContext, Channel channel, Uint8List data) {
    byteBuf.add(data);
    var decodeResult = decoder.decode(channelContext, byteBuf);

    if (!decodeResult.isDone) {
      return;
    }

    byteBuf.clearRead();

    if (decodeResult.forward != null) {
      ByteBuf buffer = ByteBuf(decodeResult.forward);

      FrameHeader? frameHeader = FrameReader.readFrameHeader(buffer);
      logger.d("Http2ClientHandler forward ${frameHeader?.type}");
      if (frameHeader?.type == FrameType.settings) {
        // 检查是否需要发送 ACK
        if (frameHeader!.hasAckFlag == false) {
          // 发送带有 ACK 标志的 SETTINGS 帧
          var ackFrame = FrameHeader(0, FrameType.settings, FLAG_ACK, 0);
          channel.writeBytes(ackFrame.encode());
        }
      }

      return;
    }

    handler.channelRead(channelContext, channel, decodeResult.data!);
  }
}

class HttpResponseHandler extends ChannelHandler<HttpResponse> {
  Completer<HttpResponse> _completer = Completer<HttpResponse>();

  @override
  void channelRead(ChannelContext channelContext, Channel channel, HttpResponse msg) {
    // log.i("[${channel.id}] Response $msg");
    _completer.complete(msg);
  }

  Future<HttpResponse> getResponse(Duration duration) {
    return _completer.future.timeout(duration);
  }

  void resetResponse() {
    _completer = Completer<HttpResponse>();
  }

  @override
  void channelInactive(ChannelContext channelContext, Channel channel) {
    // log.i("[${channel.id}] channelInactive");
  }
}
