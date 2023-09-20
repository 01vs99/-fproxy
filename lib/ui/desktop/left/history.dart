import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';

import 'package:date_format/date_format.dart';
import 'package:file_selector/file_selector.dart';
import 'package:flutter/material.dart';
import 'package:flutter_toastr/flutter_toastr.dart';
import 'package:network_proxy/network/bin/server.dart';
import 'package:network_proxy/network/channel.dart';
import 'package:network_proxy/network/handler.dart';
import 'package:network_proxy/network/http/http.dart';
import 'package:network_proxy/storage/histories.dart';
import 'package:network_proxy/ui/component/utils.dart';
import 'package:network_proxy/ui/component/widgets.dart';
import 'package:network_proxy/utils/har.dart';

import '../../content/panel.dart';
import 'domain.dart';

///历史记录
class HistoryPageWidget extends StatelessWidget {
  final ProxyServer proxyServer;
  final GlobalKey<DomainWidgetState> domainWidgetState;
  final NetworkTabController panel;

  const HistoryPageWidget({super.key, required this.proxyServer, required this.domainWidgetState, required this.panel});

  @override
  Widget build(BuildContext context) {
    return Navigator(
      onGenerateRoute: (settings) {
        switch (settings.name) {
          case "/domain":
            return MaterialPageRoute(builder: (_) => domainWidget(settings.arguments as Map));
          default:
            return MaterialPageRoute(
                builder: (_) => futureWidget(
                      HistoryStorage.instance,
                      (storage) => _HistoryWidget(storage,
                          container: domainWidgetState.currentState!.container, proxyServer: proxyServer),
                    ));
        }
      },
    );
  }

  Widget domainWidget(Map arguments) {
    HistoryItem item = arguments['item'];
    return Scaffold(
        appBar: PreferredSize(
            preferredSize: const Size.fromHeight(40),
            child: AppBar(
              leading: BackButton(style: ButtonStyle(iconSize: MaterialStateProperty.all(15))),
              centerTitle: false,
              title: Text('${item.name} 记录数 ${item.requestLength}', style: const TextStyle(fontSize: 14)),
            )),
        body: futureWidget(HistoryStorage.instance.then((value) => value.getRequests(item)), (data) {
          print("START ${DateTime.now()}");
          return DomainWidget(panel: panel, proxyServer: proxyServer, list: data, shrinkWrap: false);
        }, loading: true));
  }
}

class _HistoryWidget extends StatefulWidget {
  // 存储
  final HistoryStorage storage;
  final List<HttpRequest> container;
  final ProxyServer proxyServer;

  const _HistoryWidget(this.storage, {required this.container, required this.proxyServer});

  @override
  State<StatefulWidget> createState() {
    return _HistoryState();
  }
}

class _HistoryState extends State<_HistoryWidget> {
  ///是否保存会话
  static bool _sessionSaved = false;
  static WriteTask? writeTask;

  // 存储
  late HistoryStorage storage;

  late List<HttpRequest> container;
  late ProxyServer proxyServer;

  @override
  void initState() {
    super.initState();
    storage = widget.storage;
    container = widget.container;
    proxyServer = widget.proxyServer;
  }

  @override
  Widget build(BuildContext context) {
    print("_HistoryState build");
    List<Widget> children = [];
    if (!_sessionSaved) {
      //当前会话未保存，是否保存当前会话
      children.add(buildSaveSession(container));
    }

    var histories = storage.histories;
    for (int i = histories.length - 1; i >= 0; i--) {
      var entry = histories.elementAt(i);
      children.add(buildItem(context, i, entry));
    }

    return Scaffold(
        appBar: PreferredSize(
            preferredSize: const Size.fromHeight(30),
            child: AppBar(
              title: const Text("历史记录", style: TextStyle(fontSize: 14)),
              actions: [TextButton(onPressed: import, child: const Text("导入"))],
            )),
        body: ListView.separated(
          itemCount: children.length,
          itemBuilder: (_, index) => children[index],
          separatorBuilder: (_, index) => const Divider(thickness: 0.3, height: 0),
        ));
  }

  //导入har
  import() async {
    const XTypeGroup typeGroup = XTypeGroup(
      label: 'Har',
      extensions: <String>['har'],
    );
    final XFile? file = await openFile(acceptedTypeGroups: <XTypeGroup>[typeGroup]);
    if (file == null) {
      return;
    }

    print(file);
    try {
      var historyItem = await storage.addHarFile(file);
      setState(() {
        Navigator.pushNamed(context, '/domain', arguments: {'item': historyItem});
        FlutterToastr.show("导入成功", context);
      });
    } catch (e, t) {
      print(e);
      print(t);
      if (context.mounted) {
        FlutterToastr.show("导入失败 $e", context);
      }
    }
  }

  //构建保存会话
  Widget buildSaveSession(List<HttpRequest> container) {
    var name = formatDate(DateTime.now(), [mm, '-', d, ' ', HH, ':', nn, ':', ss]);

    return ListTile(
        dense: true,
        title: Text(name),
        subtitle: Text("当前会话未保存 记录数 ${container.length}"),
        trailing: TextButton.icon(
          icon: const Icon(Icons.save),
          label: const Text("保存"),
          onPressed: () async {
            await _writeHarFile(container, name);
            setState(() {
              _sessionSaved = true;
            });
          },
        ),
        onTap: () {});
  }

  //构建历史记录
  Widget buildItem(BuildContext context, int index, HistoryItem item) {
    return GestureDetector(
        onSecondaryTapDown: (details) => {
              showContextMenu(context, details.globalPosition, items: [
                CustomPopupMenuItem(
                    height: 35, child: const Text('导出', style: TextStyle(fontSize: 13)), onTap: () => export(item)),
                CustomPopupMenuItem(
                    height: 35,
                    child: const Text("重命名", style: TextStyle(fontSize: 13)),
                    onTap: () => renameHistory(storage, item)),
                const PopupMenuDivider(height: 0.3),
                CustomPopupMenuItem(
                    height: 35,
                    child: const Text('删除', style: TextStyle(fontSize: 13)),
                    onTap: () {
                      setState(() {
                        if (item == writeTask?.history) {
                          writeTask?.timer?.cancel();
                          writeTask?.open.close();
                          writeTask = null;
                        }
                        storage.removeHistory(index);
                        FlutterToastr.show('删除成功', context);
                      });
                    }),
              ])
            },
        child: ListTile(
            dense: true,
            title: Text(item.name),
            subtitle: Text("记录数 ${item.requestLength}  文件 ${item.size}"),
            onTap: () {
              Navigator.pushNamed(context, '/domain', arguments: {'item': item})
                  .whenComplete(() => Future.delayed(const Duration(seconds: 60), () => item.requests = null));
            }));
  }

  //重命名
  renameHistory(HistoryStorage storage, HistoryItem item) {
    String name = item.name;
    showDialog(
        context: context,
        builder: (context) {
          return AlertDialog(
            content: TextFormField(
              initialValue: name,
              decoration: const InputDecoration(label: Text("名称")),
              onChanged: (val) => name = val,
            ),
            actions: <Widget>[
              TextButton(onPressed: () => Navigator.pop(context), child: const Text("取消")),
              TextButton(
                child: const Text('保存'),
                onPressed: () {
                  if (name.isEmpty) {
                    FlutterToastr.show('名称不能为空', context, position: 2);
                    return;
                  }
                  Navigator.maybePop(context);
                  setState(() {
                    item.name = name;
                    storage.refresh();
                  });
                },
              ),
            ],
          );
        });
  }

  //导出har
  export(HistoryItem item) async {
    //文件名称
    String fileName =
        '${item.name.contains("ProxyPin") ? '' : 'ProxyPin'}${item.name}.har'.replaceAll(" ", "_").replaceAll(":", "_");
    final FileSaveLocation? result = await getSaveLocation(suggestedName: fileName);
    if (result == null) {
      return;
    }

    //获取请求
    List<HttpRequest> requests = await storage.getRequests(item);
    var file = await File(result.path).create();
    Har.writeFile(requests, file, title: item.name);
    Future.delayed(const Duration(seconds: 30), () => item.requests = null);
  }

  //写入文件
  _writeHarFile(List<HttpRequest> container, String name) async {
    var file = await HistoryStorage.openFile("${DateTime.now().millisecondsSinceEpoch}.txt");
    print(file);
    RandomAccessFile open = await file.open(mode: FileMode.append);
    HistoryItem item = await storage.addHistory(name, file, 0);
    writeTask = WriteTask(item, open, storage, callback: () => setState(() {}));
    writeTask?.writeList.addAll(container);
    proxyServer.addListener(writeTask!);
    await writeTask?.writeTask();

    writeTask?.startTask();
    setState(() {});
  }
}

///写入任务
class WriteTask implements EventListener {
  final HistoryStorage historyStorage;
  final RandomAccessFile open;
  Queue writeList = Queue();
  Timer? timer;
  final Function? callback;
  final HistoryItem history;

  WriteTask(this.history, this.open, this.historyStorage, {this.callback});

  @override
  void onRequest(Channel channel, HttpRequest request) {}

  @override
  void onResponse(Channel channel, HttpResponse response) {
    if (response.request == null) {
      return;
    }
    writeList.add(response.request!);
  }

  //写入任务
  startTask() {
    timer = Timer.periodic(const Duration(seconds: 15), (it) => writeTask());
  }

  //写入任务
  writeTask() async {
    if (writeList.isEmpty) {
      return;
    }

    int length = history.requestLength;

    while (writeList.isNotEmpty) {
      var request = writeList.removeFirst();
      var har = Har.toHar(request);

      await open.writeString(jsonEncode(har));
      await open.writeString(",\n");
      length++;
    }

    await open.flush(); //刷新

    history.requestLength = length;
    history.fileSize = await open.length();
    await historyStorage.refresh();
    callback?.call();
  }
}
