import 'package:flutter/material.dart';
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:network_proxy/network/bin/server.dart';
import 'package:network_proxy/ui/configuration.dart';
import 'package:network_proxy/ui/desktop/preference.dart';
import 'package:url_launcher/url_launcher.dart';

class LeftNavigationBar extends StatefulWidget {
  final AppConfiguration appConfiguration;
  final ProxyServer proxyServer;
  final PageController controller;
  final ValueNotifier<int> selectIndex;

  const LeftNavigationBar(
      {super.key,
      required this.appConfiguration,
      required this.proxyServer,
      required this.controller,
      required this.selectIndex});

  @override
  State<StatefulWidget> createState() {
    return _LeftNavigationBarState();
  }
}

class _LeftNavigationBarState extends State<LeftNavigationBar> {
  AppLocalizations get localizations => AppLocalizations.of(context)!;

  List<NavigationRailDestination> get destinations => [
        NavigationRailDestination(
            padding: const EdgeInsets.only(bottom: 3),
            icon: const Icon(Icons.workspaces),
            label: Text(localizations.requests, style: Theme.of(context).textTheme.bodySmall)),
        NavigationRailDestination(
            padding: const EdgeInsets.only(bottom: 3),
            icon: const Icon(Icons.favorite),
            label: Text(localizations.favorites, style: Theme.of(context).textTheme.bodySmall)),
        NavigationRailDestination(
            padding: const EdgeInsets.only(bottom: 3),
            icon: const Icon(Icons.history),
            label: Text(localizations.history, style: Theme.of(context).textTheme.bodySmall)),
        NavigationRailDestination(
            icon: const Icon(Icons.construction),
            label: Text(localizations.toolbox, style: Theme.of(context).textTheme.bodySmall)),
      ];

  @override
  Widget build(BuildContext context) {
    widget.controller.addListener(() {
      print('page: ${widget.controller.page}');
    });

    return ValueListenableBuilder(
        valueListenable: widget.selectIndex,
        builder: (_, index, __) {
          if (index == -1) {
            return const SizedBox();
          }
          print('index: $index');
          return Container(
            width: localizations.localeName == 'zh' ? 58 : 72,
            decoration:
                BoxDecoration(border: Border(right: BorderSide(color: Theme.of(context).dividerColor, width: 0.2))),
            child: Column(children: <Widget>[
              SizedBox(
                height: 320,
                child: leftNavigation(index),
              ),
              Expanded(
                  child: Column(
                mainAxisAlignment: MainAxisAlignment.end,
                children: [
                  Tooltip(
                      message: localizations.preference,
                      preferBelow: false,
                      child: IconButton(
                          onPressed: () {
                            showDialog(
                                context: context,
                                builder: (_) => Preference(widget.appConfiguration, widget.proxyServer.configuration));
                          },
                          icon: Icon(Icons.settings_outlined, color: Colors.grey.shade500))),
                  const SizedBox(height: 5),
                  Tooltip(
                      preferBelow: true,
                      message: localizations.feedback,
                      child: IconButton(
                        onPressed: () =>
                            launchUrl(Uri.parse("https://github.com/wanghongenpin/network_proxy_flutter/issues")),
                        icon: Icon(Icons.feedback_outlined, color: Colors.grey.shade500),
                      )),
                  const SizedBox(height: 10),
                ],
              ))
            ]),
          );
        });
  }

  //left menu eg: requests, favorites, history, toolbox
  Widget leftNavigation(int index) {
    return NavigationRail(
        minWidth: 58,
        backgroundColor: Theme.of(context).scaffoldBackgroundColor,
        selectedIconTheme: IconThemeData(color: Theme.of(context).colorScheme.primary),
        labelType: NavigationRailLabelType.all,
        destinations: destinations,
        selectedIndex: index,
        onDestinationSelected: (int index) {
          widget.controller.jumpToPage(index);
          widget.selectIndex.value = index;
        });
  }
}
